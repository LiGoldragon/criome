use criome::language::{
    AgreementFact, AgreementRule, Contract, Decision, Evidence, Rule, Threshold, TimeSwitch,
    TimedRule,
};
use signal_criome::{Identity, ObjectDigest, RequiredSignatureThreshold, TimestampNanos};

fn developer(name: &str) -> Identity {
    Identity::developer(name.to_owned())
}

fn cluster(name: &str) -> Identity {
    Identity::cluster(name.to_owned())
}

fn moment(value: u64) -> TimestampNanos {
    TimestampNanos::new(value)
}

fn digest(value: &[u8]) -> ObjectDigest {
    ObjectDigest::from_bytes(value)
}

#[test]
fn threshold_contract_accepts_only_enough_distinct_authorities() {
    let contract = Contract::new(Rule::threshold(Threshold::new(
        RequiredSignatureThreshold::new(2),
        vec![
            developer("operator"),
            developer("designer"),
            developer("auditor"),
        ],
    )));

    let one_signature = Evidence::new(moment(10)).with_signature(developer("operator"));
    let duplicate_signature = Evidence::new(moment(10))
        .with_signature(developer("operator"))
        .with_signature(developer("operator"));
    let two_signatures = Evidence::new(moment(10))
        .with_signature(developer("operator"))
        .with_signature(developer("designer"));

    assert_eq!(contract.evaluate(&one_signature), Decision::Rejected);
    assert_eq!(contract.evaluate(&duplicate_signature), Decision::Rejected);
    assert_eq!(contract.evaluate(&two_signatures), Decision::Authorized);
}

#[test]
fn time_switch_changes_quorum_after_boundary() {
    let contract = Contract::new(Rule::time_switch(TimeSwitch::new(
        moment(100),
        Rule::threshold(Threshold::new(
            RequiredSignatureThreshold::new(1),
            vec![developer("operator"), developer("designer")],
        )),
        Rule::threshold(Threshold::new(
            RequiredSignatureThreshold::new(2),
            vec![developer("operator"), developer("designer")],
        )),
    )));
    let operator_signature = Evidence::new(moment(50)).with_signature(developer("operator"));
    let late_operator_signature = Evidence::new(moment(150)).with_signature(developer("operator"));
    let late_quorum = Evidence::new(moment(150))
        .with_signature(developer("operator"))
        .with_signature(developer("designer"));

    assert_eq!(contract.evaluate(&operator_signature), Decision::Authorized);
    assert_eq!(
        contract.evaluate(&late_operator_signature),
        Decision::Rejected
    );
    assert_eq!(contract.evaluate(&late_quorum), Decision::Authorized);
}

#[test]
fn active_after_rule_models_timelock_release() {
    let contract = Contract::new(Rule::active_after(TimedRule::new(
        moment(100),
        Rule::signed_by(developer("operator")),
    )));
    let early_signature = Evidence::new(moment(99)).with_signature(developer("operator"));
    let released_signature = Evidence::new(moment(100)).with_signature(developer("operator"));

    assert_eq!(contract.evaluate(&early_signature), Decision::Rejected);
    assert_eq!(contract.evaluate(&released_signature), Decision::Authorized);
}

#[test]
fn agreement_rule_accepts_only_matching_resolver_fact() {
    let divergence = digest(b"network fork");
    let resolution = digest(b"chosen canonical branch");
    let resolver = cluster("model-governance-panel");
    let contract = Contract::new(Rule::agreement(AgreementRule::new(
        divergence.clone(),
        resolution.clone(),
        resolver.clone(),
    )));
    let wrong_resolution = Evidence::new(moment(20)).with_agreement(AgreementFact::new(
        divergence.clone(),
        digest(b"other branch"),
        resolver.clone(),
    ));
    let wrong_resolver = Evidence::new(moment(20)).with_agreement(AgreementFact::new(
        divergence.clone(),
        resolution.clone(),
        developer("single-reviewer"),
    ));
    let matching = Evidence::new(moment(20))
        .with_agreement(AgreementFact::new(divergence, resolution, resolver));

    assert_eq!(contract.evaluate(&wrong_resolution), Decision::Rejected);
    assert_eq!(contract.evaluate(&wrong_resolver), Decision::Rejected);
    assert_eq!(contract.evaluate(&matching), Decision::Authorized);
}

#[test]
fn schema_sketch_names_every_poc_construct() {
    let schema = include_str!("../schema/criome.language.schema");

    for construct in [
        "KeyAtom",
        "Contract",
        "Rule",
        "Threshold",
        "TimedRule",
        "TimeSwitch",
        "AgreementRule",
        "Evidence",
        "AgreementFact",
        "Decision",
    ] {
        assert!(
            schema.contains(construct),
            "schema sketch should name {construct}"
        );
    }
}
