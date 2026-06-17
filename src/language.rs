//! Crayome internal language proof of concept.
//!
//! The module models the eventual Criome policy language as a constrained
//! expression tree over identity evidence. It is not on the public
//! `signal-criome` wire yet; it is compiled design pressure for the schema in
//! `schema/crayome.language.schema`.

use signal_criome::{Identity, ObjectDigest, RequiredSignatureThreshold, TimestampNanos};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contract {
    rule: Rule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rule {
    SignedBy(Identity),
    All(Vec<Rule>),
    Any(Vec<Rule>),
    Threshold(Threshold),
    ActiveAfter(TimedRule),
    ActiveUntil(TimedRule),
    TimeSwitch(TimeSwitch),
    Agreement(AgreementRule),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Threshold {
    required_signatures: RequiredSignatureThreshold,
    authorities: Vec<Identity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimedRule {
    boundary: TimestampNanos,
    rule: Box<Rule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSwitch {
    boundary: TimestampNanos,
    before: Box<Rule>,
    after: Box<Rule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgreementRule {
    divergence: ObjectDigest,
    resolution: ObjectDigest,
    resolver: Identity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence {
    observed_at: TimestampNanos,
    signatures: Vec<Identity>,
    agreements: Vec<AgreementFact>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgreementFact {
    divergence: ObjectDigest,
    resolution: ObjectDigest,
    resolver: Identity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Authorized,
    Rejected,
}

impl Contract {
    pub fn new(rule: Rule) -> Self {
        Self { rule }
    }

    pub fn evaluate(&self, evidence: &Evidence) -> Decision {
        if self.rule.satisfied_by(evidence) {
            Decision::Authorized
        } else {
            Decision::Rejected
        }
    }

    pub fn rule(&self) -> &Rule {
        &self.rule
    }
}

impl Rule {
    pub fn signed_by(identity: Identity) -> Self {
        Self::SignedBy(identity)
    }

    pub fn all(rules: Vec<Rule>) -> Self {
        Self::All(rules)
    }

    pub fn any(rules: Vec<Rule>) -> Self {
        Self::Any(rules)
    }

    pub fn threshold(threshold: Threshold) -> Self {
        Self::Threshold(threshold)
    }

    pub fn active_after(timed_rule: TimedRule) -> Self {
        Self::ActiveAfter(timed_rule)
    }

    pub fn active_until(timed_rule: TimedRule) -> Self {
        Self::ActiveUntil(timed_rule)
    }

    pub fn time_switch(time_switch: TimeSwitch) -> Self {
        Self::TimeSwitch(time_switch)
    }

    pub fn agreement(agreement: AgreementRule) -> Self {
        Self::Agreement(agreement)
    }

    pub fn satisfied_by(&self, evidence: &Evidence) -> bool {
        match self {
            Self::SignedBy(identity) => evidence.has_signature_from(identity),
            Self::All(rules) => rules.iter().all(|rule| rule.satisfied_by(evidence)),
            Self::Any(rules) => rules.iter().any(|rule| rule.satisfied_by(evidence)),
            Self::Threshold(threshold) => threshold.satisfied_by(evidence),
            Self::ActiveAfter(timed_rule) => {
                evidence.observed_at().into_u64() >= timed_rule.boundary().into_u64()
                    && timed_rule.rule().satisfied_by(evidence)
            }
            Self::ActiveUntil(timed_rule) => {
                evidence.observed_at().into_u64() < timed_rule.boundary().into_u64()
                    && timed_rule.rule().satisfied_by(evidence)
            }
            Self::TimeSwitch(time_switch) => {
                time_switch.active_rule(evidence).satisfied_by(evidence)
            }
            Self::Agreement(agreement) => evidence.has_agreement_for(agreement),
        }
    }
}

impl Threshold {
    pub fn new(
        required_signatures: RequiredSignatureThreshold,
        authorities: Vec<Identity>,
    ) -> Self {
        Self {
            required_signatures,
            authorities,
        }
    }

    pub fn satisfied_by(&self, evidence: &Evidence) -> bool {
        self.satisfied_count(evidence) >= self.required_signatures.into_u16()
    }

    pub fn satisfied_count(&self, evidence: &Evidence) -> u16 {
        self.authorities
            .iter()
            .filter(|authority| evidence.has_signature_from(authority))
            .count() as u16
    }

    pub const fn required_signatures(&self) -> RequiredSignatureThreshold {
        self.required_signatures
    }

    pub fn authorities(&self) -> &[Identity] {
        self.authorities.as_slice()
    }
}

impl TimedRule {
    pub fn new(boundary: TimestampNanos, rule: Rule) -> Self {
        Self {
            boundary,
            rule: Box::new(rule),
        }
    }

    pub const fn boundary(&self) -> TimestampNanos {
        self.boundary
    }

    pub fn rule(&self) -> &Rule {
        self.rule.as_ref()
    }
}

impl TimeSwitch {
    pub fn new(boundary: TimestampNanos, before: Rule, after: Rule) -> Self {
        Self {
            boundary,
            before: Box::new(before),
            after: Box::new(after),
        }
    }

    pub fn active_rule(&self, evidence: &Evidence) -> &Rule {
        if evidence.observed_at().into_u64() < self.boundary.into_u64() {
            self.before.as_ref()
        } else {
            self.after.as_ref()
        }
    }

    pub const fn boundary(&self) -> TimestampNanos {
        self.boundary
    }
}

impl AgreementRule {
    pub fn new(divergence: ObjectDigest, resolution: ObjectDigest, resolver: Identity) -> Self {
        Self {
            divergence,
            resolution,
            resolver,
        }
    }

    pub fn matches(&self, fact: &AgreementFact) -> bool {
        self.divergence == fact.divergence
            && self.resolution == fact.resolution
            && self.resolver == fact.resolver
    }

    pub fn divergence(&self) -> &ObjectDigest {
        &self.divergence
    }

    pub fn resolution(&self) -> &ObjectDigest {
        &self.resolution
    }

    pub fn resolver(&self) -> &Identity {
        &self.resolver
    }
}

impl Evidence {
    pub fn new(observed_at: TimestampNanos) -> Self {
        Self {
            observed_at,
            signatures: Vec::new(),
            agreements: Vec::new(),
        }
    }

    pub fn with_signature(mut self, identity: Identity) -> Self {
        if !self.signatures.contains(&identity) {
            self.signatures.push(identity);
        }
        self
    }

    pub fn with_agreement(mut self, fact: AgreementFact) -> Self {
        if !self.agreements.contains(&fact) {
            self.agreements.push(fact);
        }
        self
    }

    pub const fn observed_at(&self) -> TimestampNanos {
        self.observed_at
    }

    pub fn has_signature_from(&self, identity: &Identity) -> bool {
        self.signatures.contains(identity)
    }

    pub fn has_agreement_for(&self, agreement: &AgreementRule) -> bool {
        self.agreements.iter().any(|fact| agreement.matches(fact))
    }

    pub fn signatures(&self) -> &[Identity] {
        self.signatures.as_slice()
    }

    pub fn agreements(&self) -> &[AgreementFact] {
        self.agreements.as_slice()
    }
}

impl AgreementFact {
    pub fn new(divergence: ObjectDigest, resolution: ObjectDigest, resolver: Identity) -> Self {
        Self {
            divergence,
            resolution,
            resolver,
        }
    }
}
