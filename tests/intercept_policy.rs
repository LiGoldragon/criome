use criome::Error;
use criome::actors::store::{
    AnswerParkedSpiritRequest, FetchParkedSpiritRequests, InterceptSpiritAuthorization,
    ReadInterceptPolicies, ReadParkedSpiritRequestHistory, StoreInterceptPolicy, StoreKernel,
};
use criome::tables::StoreLocation;
use kameo::actor::Spawn;
use signal_criome::{
    ApprovalAuditSource, ExpiryAction, InterceptPolicy, InterceptPolicyProposal,
    InterceptTargetSelector, MentciSessionSlot, ParkedRequestAnswer, ParkedRequestDecision,
    ParkedRequestOutcome, ParkedRequestQuery, PolicyDurationNanos, PolicyOverlapMode,
    PolicyPriority, RawSpiritOperationPayload, SpiritAuthorizationContext, SpiritOperationName,
    SpiritOperationNames, SpiritProcessKey, TimestampNanos,
};

fn fixture_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("criome-intercept-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create fixture dir");
    path
}

fn store_location(name: &str) -> StoreLocation {
    StoreLocation::new(fixture_path(name).join("criome.sema"))
}

fn timestamp(value: u64) -> TimestampNanos {
    TimestampNanos::new(value)
}

fn proposal(
    session: &str,
    target: &str,
    operation: &str,
    priority: u64,
    expiry_action: ExpiryAction,
    overlap_mode: PolicyOverlapMode,
) -> InterceptPolicyProposal {
    InterceptPolicyProposal {
        mentci_session_slot: MentciSessionSlot::new(session),
        intercept_target_selector: InterceptTargetSelector::new(SpiritProcessKey::new(target)),
        spirit_operation_names: SpiritOperationNames::from_names(vec![SpiritOperationName::new(
            operation,
        )]),
        policy_duration_nanos: PolicyDurationNanos::new(100),
        expiry_action,
        policy_priority: PolicyPriority::new(priority),
        policy_overlap_mode: overlap_mode,
    }
}

fn context(target: &str, operation: &str, payload: &str) -> SpiritAuthorizationContext {
    SpiritAuthorizationContext {
        spirit_operation_name: SpiritOperationName::new(operation),
        raw_spirit_operation_payload: RawSpiritOperationPayload::new(payload),
        spirit_process_key: SpiritProcessKey::new(target),
    }
}

fn all_parked_requests() -> ParkedRequestQuery {
    ParkedRequestQuery {
        optional_mentci_session_slot: None,
        optional_intercept_target_selector: None,
    }
}

fn policy_identifier(policy: &InterceptPolicy) -> String {
    policy.intercept_policy_identifier.as_str().to_owned()
}

#[tokio::test]
async fn intercept_policy_matching_uses_highest_priority_and_rejects_same_priority_overlap() {
    let store = StoreKernel::spawn(store_location("priority-overlap"));

    let low = store
        .ask(StoreInterceptPolicy::create(
            proposal(
                "mentci-a",
                "spirit-main",
                "Record",
                1,
                ExpiryAction::LeaveParked,
                PolicyOverlapMode::RejectSamePriorityOverlap,
            ),
            timestamp(10),
        ))
        .await
        .expect("store low priority policy")
        .into_policy()
        .into_policy();
    let high = store
        .ask(StoreInterceptPolicy::create(
            proposal(
                "mentci-b",
                "spirit-main",
                "Record",
                3,
                ExpiryAction::LeaveParked,
                PolicyOverlapMode::RejectSamePriorityOverlap,
            ),
            timestamp(10),
        ))
        .await
        .expect("store high priority policy")
        .into_policy()
        .into_policy();

    let overlap_error = match store
        .ask(StoreInterceptPolicy::create(
            proposal(
                "mentci-c",
                "spirit-main",
                "Record",
                3,
                ExpiryAction::LeaveParked,
                PolicyOverlapMode::RejectSamePriorityOverlap,
            ),
            timestamp(11),
        ))
        .await
    {
        Ok(_) => panic!("same-priority overlap should reject"),
        Err(error) => error,
    };
    assert!(matches!(
        overlap_error,
        kameo::error::SendError::HandlerError(Error::InterceptPolicyOverlapRejected)
    ));

    let replacement = store
        .ask(StoreInterceptPolicy::replace(
            proposal(
                "mentci-d",
                "spirit-main",
                "Record",
                3,
                ExpiryAction::LeaveParked,
                PolicyOverlapMode::RejectSamePriorityOverlap,
            ),
            timestamp(12),
        ))
        .await
        .expect("replace same-priority overlap")
        .into_policy()
        .into_policy();

    let policies = store
        .ask(ReadInterceptPolicies::new(timestamp(13)))
        .await
        .expect("list active policies")
        .into_policies();
    let active_identifiers: Vec<_> = policies.policies().iter().map(policy_identifier).collect();
    assert_eq!(
        active_identifiers,
        vec![policy_identifier(&low), policy_identifier(&replacement)]
    );
    assert!(!active_identifiers.contains(&policy_identifier(&high)));

    let wrong_operation = store
        .ask(InterceptSpiritAuthorization::new(
            context("spirit-main", "Delete", "(Delete example)"),
            timestamp(14),
        ))
        .await
        .expect("wrong operation evaluates cleanly")
        .into_request();
    assert!(wrong_operation.is_none());

    let wrong_target = store
        .ask(InterceptSpiritAuthorization::new(
            context("other-spirit", "Record", "(Record example)"),
            timestamp(14),
        ))
        .await
        .expect("wrong target evaluates cleanly")
        .into_request();
    assert!(wrong_target.is_none());

    let parked = store
        .ask(InterceptSpiritAuthorization::new(
            context("spirit-main", "Record", "(Record example)"),
            timestamp(14),
        ))
        .await
        .expect("intercept request")
        .into_request()
        .expect("matching policy parks request");
    assert_eq!(
        parked.request().intercept_policy_identifier,
        replacement.intercept_policy_identifier.clone()
    );
    assert_eq!(
        parked.request().mentci_session_slot,
        replacement.mentci_session_slot
    );
}

#[tokio::test]
async fn one_spirit_process_can_hold_many_operation_policies_with_set_time_expiry() {
    let store = StoreKernel::spawn(store_location("many-policies-per-process"));

    let record_policy = store
        .ask(StoreInterceptPolicy::create(
            proposal(
                "mentci-alpha-record",
                "spirit-alpha",
                "Record",
                4,
                ExpiryAction::LeaveParked,
                PolicyOverlapMode::RejectSamePriorityOverlap,
            ),
            timestamp(20),
        ))
        .await
        .expect("store record policy")
        .into_policy()
        .into_policy();
    let observe_policy = store
        .ask(StoreInterceptPolicy::create(
            proposal(
                "mentci-alpha-observe",
                "spirit-alpha",
                "Observe",
                4,
                ExpiryAction::LeaveParked,
                PolicyOverlapMode::RejectSamePriorityOverlap,
            ),
            timestamp(21),
        ))
        .await
        .expect("store observe policy")
        .into_policy()
        .into_policy();
    let beta_policy = store
        .ask(StoreInterceptPolicy::create(
            proposal(
                "mentci-beta-record",
                "spirit-beta",
                "Record",
                4,
                ExpiryAction::LeaveParked,
                PolicyOverlapMode::RejectSamePriorityOverlap,
            ),
            timestamp(22),
        ))
        .await
        .expect("store beta policy")
        .into_policy()
        .into_policy();

    assert_eq!(
        record_policy.intercept_policy_window.starts_at,
        timestamp(20)
    );
    assert_eq!(
        record_policy.intercept_policy_window.expires_at,
        timestamp(120)
    );
    assert_eq!(
        observe_policy.intercept_policy_window.starts_at,
        timestamp(21)
    );
    assert_eq!(
        observe_policy.intercept_policy_window.expires_at,
        timestamp(121)
    );
    assert_eq!(beta_policy.intercept_policy_window.starts_at, timestamp(22));
    assert_eq!(
        beta_policy.intercept_policy_window.expires_at,
        timestamp(122)
    );

    let record_request = store
        .ask(InterceptSpiritAuthorization::new(
            context("spirit-alpha", "Record", "(Record alpha)"),
            timestamp(23),
        ))
        .await
        .expect("intercept alpha record")
        .into_request()
        .expect("alpha record parked")
        .request()
        .clone();
    let observe_request = store
        .ask(InterceptSpiritAuthorization::new(
            context("spirit-alpha", "Observe", "(Observe alpha)"),
            timestamp(24),
        ))
        .await
        .expect("intercept alpha observe")
        .into_request()
        .expect("alpha observe parked")
        .request()
        .clone();
    let beta_request = store
        .ask(InterceptSpiritAuthorization::new(
            context("spirit-beta", "Record", "(Record beta)"),
            timestamp(25),
        ))
        .await
        .expect("intercept beta record")
        .into_request()
        .expect("beta record parked")
        .request()
        .clone();

    assert_eq!(
        record_request.intercept_policy_identifier,
        record_policy.intercept_policy_identifier
    );
    assert_eq!(
        observe_request.intercept_policy_identifier,
        observe_policy.intercept_policy_identifier
    );
    assert_eq!(
        beta_request.intercept_policy_identifier,
        beta_policy.intercept_policy_identifier
    );

    let alpha_snapshot = store
        .ask(FetchParkedSpiritRequests::new(
            ParkedRequestQuery {
                optional_mentci_session_slot: None,
                optional_intercept_target_selector: Some(InterceptTargetSelector::new(
                    SpiritProcessKey::new("spirit-alpha"),
                )),
            },
            timestamp(26),
        ))
        .await
        .expect("fetch alpha parked requests")
        .into_snapshot();
    assert_eq!(
        alpha_snapshot.requests(),
        &[record_request.clone(), observe_request.clone()]
    );

    let observe_session_snapshot = store
        .ask(FetchParkedSpiritRequests::new(
            ParkedRequestQuery {
                optional_mentci_session_slot: Some(MentciSessionSlot::new("mentci-alpha-observe")),
                optional_intercept_target_selector: None,
            },
            timestamp(27),
        ))
        .await
        .expect("fetch observe-session parked request")
        .into_snapshot();
    assert_eq!(observe_session_snapshot.requests(), &[observe_request]);
}

#[tokio::test]
async fn expiry_actions_resolve_or_keep_parked_requests_with_audit_source() {
    let store = StoreKernel::spawn(store_location("expiry-actions"));

    for (session, target, operation, action) in [
        (
            "mentci-auto-approve",
            "spirit-approve",
            "Append",
            ExpiryAction::AutoApprove,
        ),
        (
            "mentci-auto-reject",
            "spirit-reject",
            "Delete",
            ExpiryAction::AutoReject,
        ),
        (
            "mentci-leave-parked",
            "spirit-leave",
            "Observe",
            ExpiryAction::LeaveParked,
        ),
    ] {
        store
            .ask(StoreInterceptPolicy::create(
                proposal(
                    session,
                    target,
                    operation,
                    1,
                    action,
                    PolicyOverlapMode::RejectSamePriorityOverlap,
                ),
                timestamp(10),
            ))
            .await
            .expect("store expiry policy");
        store
            .ask(InterceptSpiritAuthorization::new(
                context(target, operation, "payload"),
                timestamp(11),
            ))
            .await
            .expect("intercept request")
            .into_request()
            .expect("request parked");
    }

    let snapshot = store
        .ask(FetchParkedSpiritRequests::new(
            all_parked_requests(),
            timestamp(111),
        ))
        .await
        .expect("expiry sweep during fetch")
        .into_snapshot();
    assert_eq!(snapshot.requests().len(), 1);
    assert_eq!(
        snapshot.requests()[0].mentci_session_slot.as_str(),
        "mentci-leave-parked"
    );

    let history = store
        .ask(ReadParkedSpiritRequestHistory)
        .await
        .expect("read parked history")
        .into_requests();
    let resolutions: Vec<_> = history
        .iter()
        .filter_map(|request| request.resolution())
        .map(|resolution| {
            (
                resolution.parked_request_outcome,
                resolution.approval_audit_source,
            )
        })
        .collect();
    assert_eq!(
        resolutions,
        vec![
            (
                ParkedRequestOutcome::Approved,
                ApprovalAuditSource::Automatic
            ),
            (
                ParkedRequestOutcome::Rejected,
                ApprovalAuditSource::Automatic
            ),
        ]
    );
}

#[tokio::test]
async fn manual_answer_resolves_one_parked_request_and_resubmission_hits_current_policy() {
    let store = StoreKernel::spawn(store_location("manual-answer"));

    store
        .ask(StoreInterceptPolicy::create(
            proposal(
                "mentci-manual",
                "spirit-main",
                "Record",
                1,
                ExpiryAction::AutoApprove,
                PolicyOverlapMode::RejectSamePriorityOverlap,
            ),
            timestamp(10),
        ))
        .await
        .expect("store policy");

    let first = store
        .ask(InterceptSpiritAuthorization::new(
            context("spirit-main", "Record", "first"),
            timestamp(11),
        ))
        .await
        .expect("intercept first")
        .into_request()
        .expect("first parked");
    let resolution = store
        .ask(AnswerParkedSpiritRequest::new(
            ParkedRequestAnswer {
                parked_request_identifier: first.request().parked_request_identifier.clone(),
                parked_request_decision: ParkedRequestDecision::Reject,
            },
            timestamp(12),
        ))
        .await
        .expect("answer first")
        .into_resolution();
    assert_eq!(
        resolution.parked_request_outcome,
        ParkedRequestOutcome::Rejected
    );
    assert_eq!(
        resolution.approval_audit_source,
        ApprovalAuditSource::Manual
    );

    let second = store
        .ask(InterceptSpiritAuthorization::new(
            context("spirit-main", "Record", "second"),
            timestamp(13),
        ))
        .await
        .expect("intercept second")
        .into_request()
        .expect("second parked");
    assert_ne!(
        first.request().parked_request_identifier,
        second.request().parked_request_identifier,
        "manual denial resolves only the first parked request"
    );

    let snapshot = store
        .ask(FetchParkedSpiritRequests::new(
            all_parked_requests(),
            timestamp(14),
        ))
        .await
        .expect("fetch active parked requests")
        .into_snapshot();
    assert_eq!(snapshot.requests(), &[second.request().clone()]);
}
