use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Common properties shared by all GitHub events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repository,
    pub payload: serde_json::Value, // Will be deserialized based on event type
    pub public: bool,
    pub created_at: String,
    pub org: Option<Organization>,
}

/// Actor who triggered the event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Actor {
    pub id: u64,
    pub login: String,
    pub display_login: Option<String>,
    pub gravatar_id: String,
    pub url: String,
    pub avatar_url: String,
}

/// Repository where the event occurred
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Repository {
    pub id: u64,
    pub name: String,
    pub url: String,
}

/// Organization (optional, appears only if applicable)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Organization {
    pub id: u64,
    pub login: String,
    pub gravatar_id: String,
    pub url: String,
    pub avatar_url: String,
}

/// Enum for all GitHub event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GitHubEventType {
    CommitCommentEvent(CommitCommentEventPayload),
    CreateEvent(CreateEventPayload),
    DeleteEvent(DeleteEventPayload),
    ForkEvent(ForkEventPayload),
    GollumEvent(GollumEventPayload),
    IssueCommentEvent(IssueCommentEventPayload),
    IssuesEvent(IssuesEventPayload),
    MemberEvent(MemberEventPayload),
    PublicEvent(PublicEventPayload),
    PullRequestEvent(PullRequestEventPayload),
    PullRequestReviewEvent(PullRequestReviewEventPayload),
    PullRequestReviewCommentEvent(PullRequestReviewCommentEventPayload),
    PullRequestReviewThreadEvent(PullRequestReviewThreadEventPayload),
    PushEvent(PushEventPayload),
    ReleaseEvent(ReleaseEventPayload),
    SponsorshipEvent(SponsorshipEventPayload),
    WatchEvent(WatchEventPayload),
}

// Event Payload Structures

/// CommitCommentEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitCommentEventPayload {
    pub action: String, // "created"
    pub comment: CommitComment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitComment {
    pub id: u64,
    pub url: String,
    pub html_url: String,
    pub body: String,
    pub user: Option<User>,
    pub created_at: String,
    pub updated_at: String,
    pub commit_id: String,
    pub path: Option<String>,
    pub position: Option<u32>,
    pub line: Option<u32>,
}

/// CreateEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEventPayload {
    pub ref_type: String, // "repository", "branch", "tag"
    #[serde(rename = "ref")]
    pub ref_name: Option<String>,
    pub master_branch: Option<String>,
    pub description: Option<String>,
    pub pusher_type: Option<String>,
}

/// DeleteEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteEventPayload {
    pub ref_type: String, // "branch", "tag"
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub pusher_type: String,
}

/// ForkEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkEventPayload {
    pub forkee: Repository,
}

/// GollumEvent payload (wiki pages)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GollumEventPayload {
    pub pages: Vec<WikiPage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiPage {
    pub page_name: String,
    pub title: String,
    pub summary: Option<String>,
    pub action: String, // "created", "edited"
    pub sha: String,
    pub html_url: String,
}

/// IssueCommentEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueCommentEventPayload {
    pub action: String, // "created", "edited", "deleted"
    pub changes: Option<Changes>,
    pub issue: Issue,
    pub comment: IssueComment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Changes {
    pub body: Option<ChangeDetail>,
    pub title: Option<ChangeDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeDetail {
    pub from: String,
}

/// IssuesEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuesEventPayload {
    pub action: String, // "opened", "edited", "closed", "reopened", "assigned", "unassigned", "labeled", "unlabeled"
    pub issue: Issue,
    pub changes: Option<Changes>,
    pub assignee: Option<User>,
    pub label: Option<Label>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Issue {
    pub id: u64,
    pub number: u32,
    pub title: String,
    pub body: Option<String>,
    pub user: Option<User>,
    pub state: String,
    pub locked: bool,
    pub assignee: Option<User>,
    pub assignees: Vec<User>,
    pub milestone: Option<Milestone>,
    pub comments: u32,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: Option<String>,
    pub author_association: String,
    pub labels: Vec<Label>,
    pub html_url: String,
    pub url: String,
    // Yes, sometimes an "issue" is actually a pull request. Great.
    pub pull_request: Option<PullRequestRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestRef {
    pub url: String,
    pub html_url: String,
    pub diff_url: String,
    pub patch_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueComment {
    pub id: u64,
    pub url: String,
    pub html_url: String,
    pub body: String,
    pub user: Option<User>,
    pub created_at: String,
    pub updated_at: String,
    pub author_association: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Label {
    pub id: u64,
    pub name: String,
    pub color: String,
    pub description: Option<String>,
    pub default: bool,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Milestone {
    pub id: u64,
    pub number: u32,
    pub title: String,
    pub description: Option<String>,
    pub creator: Option<User>,
    pub open_issues: u32,
    pub closed_issues: u32,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
    pub due_on: Option<String>,
    pub closed_at: Option<String>,
    pub url: String,
    pub html_url: String,
}

/// MemberEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberEventPayload {
    pub action: String, // "added", "removed", "edited"
    pub member: User,
    pub changes: Option<MemberChanges>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberChanges {
    pub permission: Option<ChangeDetail>,
}

/// PublicEvent payload (when a private repository is made public)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicEventPayload {
    // Empty payload
}

/// PullRequestEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestEventPayload {
    pub action: String, // "opened", "edited", "closed", "reopened", "assigned", "unassigned", "review_requested", "review_request_removed", "labeled", "unlabeled", "synchronize"
    pub number: u32,
    pub changes: Option<Changes>,
    pub pull_request: PullRequest,
    pub assignee: Option<User>,
    pub requested_reviewer: Option<User>,
    pub requested_team: Option<Team>,
    pub label: Option<Label>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequest {
    pub id: u64,
    pub number: u32,
    pub title: String,
    pub body: Option<String>,
    pub user: Option<User>,
    pub state: String,
    pub locked: bool,
    pub assignee: Option<User>,
    pub assignees: Vec<User>,
    pub requested_reviewers: Vec<User>,
    pub requested_teams: Vec<Team>,
    pub milestone: Option<Milestone>,
    pub head: PullRequestBranch,
    pub base: PullRequestBranch,
    pub merged: bool,
    pub mergeable: Option<bool>,
    pub rebaseable: Option<bool>,
    pub mergeable_state: String,
    pub merged_by: Option<User>,
    pub comments: u32,
    pub review_comments: u32,
    pub maintainer_can_modify: bool,
    pub commits: u32,
    pub additions: u32,
    pub deletions: u32,
    pub changed_files: u32,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: Option<String>,
    pub merged_at: Option<String>,
    pub merge_commit_sha: Option<String>,
    pub author_association: String,
    pub draft: bool,
    pub html_url: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestBranch {
    pub label: String,
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub sha: String,
    pub user: Option<User>,
    pub repo: Repository,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Team {
    pub id: u64,
    pub name: String,
    pub slug: String,
    pub description: Option<String>,
    pub privacy: String,
    pub permission: String,
    pub url: String,
    pub html_url: String,
    pub members_url: String,
    pub repositories_url: String,
}

/// PullRequestReviewEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestReviewEventPayload {
    pub action: String, // "submitted", "edited", "dismissed"
    pub review: PullRequestReview,
    pub pull_request: PullRequest,
    pub changes: Option<ReviewChanges>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestReview {
    pub id: u64,
    pub user: Option<User>,
    pub body: Option<String>,
    pub state: String,
    pub html_url: String,
    pub pull_request_url: String,
    pub author_association: String,
    pub submitted_at: String,
    pub commit_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewChanges {
    pub body: Option<ChangeDetail>,
}

/// PullRequestReviewCommentEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestReviewCommentEventPayload {
    pub action: String, // "created", "edited", "deleted"
    pub changes: Option<Changes>,
    pub pull_request: PullRequest,
    pub comment: PullRequestReviewComment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestReviewComment {
    pub id: u64,
    pub url: String,
    pub html_url: String,
    pub pull_request_url: String,
    pub body: String,
    pub user: Option<User>,
    pub created_at: String,
    pub updated_at: String,
    pub author_association: String,
    pub commit_id: String,
    pub original_commit_id: String,
    pub diff_hunk: String,
    pub path: String,
    pub position: Option<u32>,
    pub original_position: Option<u32>,
    pub line: Option<u32>,
    pub original_line: Option<u32>,
    pub start_line: Option<u32>,
    pub original_start_line: Option<u32>,
    pub side: String,
    pub start_side: Option<String>,
    pub pull_request_review_id: Option<u64>,
}

/// PullRequestReviewThreadEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestReviewThreadEventPayload {
    pub action: String, // "resolved", "unresolved"
    pub pull_request: PullRequest,
    pub thread: ReviewThread,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewThread {
    pub node_id: String,
    pub comments: Vec<PullRequestReviewComment>,
}

/// PushEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushEventPayload {
    pub push_id: u64,
    pub size: u32,
    pub distinct_size: u32,
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub head: String,
    pub before: String,
    pub commits: Vec<PushCommit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushCommit {
    pub sha: String,
    pub message: String,
    pub author: CommitAuthor,
    pub url: String,
    pub distinct: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitAuthor {
    pub name: String,
    pub email: String,
}

/// ReleaseEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseEventPayload {
    pub action: String, // "published", "unpublished", "created", "edited", "deleted", "prereleased", "released"
    pub changes: Option<ReleaseChanges>,
    pub release: Release,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseChanges {
    pub body: Option<ChangeDetail>,
    pub name: Option<ChangeDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Release {
    pub id: u64,
    pub tag_name: String,
    pub target_commitish: String,
    pub name: Option<String>,
    pub body: Option<String>,
    pub draft: bool,
    pub prerelease: bool,
    pub created_at: String,
    pub published_at: Option<String>,
    pub author: User,
    pub assets: Vec<ReleaseAsset>,
    pub tarball_url: Option<String>,
    pub zipball_url: Option<String>,
    pub html_url: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseAsset {
    pub id: u64,
    pub name: String,
    pub label: Option<String>,
    pub uploader: User,
    pub content_type: String,
    pub state: String,
    pub size: u64,
    pub download_count: u64,
    pub created_at: String,
    pub updated_at: String,
    pub browser_download_url: String,
    pub url: String,
}

/// SponsorshipEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SponsorshipEventPayload {
    pub action: String, // "created", "cancelled", "edited", "tier_changed", "pending_cancellation", "pending_tier_change"
    pub effective_date: Option<String>,
    pub changes: Option<SponsorshipChanges>,
    pub sponsorship: Sponsorship,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SponsorshipChanges {
    pub tier: Option<TierChange>,
    pub privacy_level: Option<ChangeDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierChange {
    pub from: SponsorshipTier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sponsorship {
    pub node_id: String,
    pub created_at: String,
    pub sponsorable: User,
    pub sponsor: User,
    pub privacy_level: String,
    pub tier: SponsorshipTier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SponsorshipTier {
    pub node_id: String,
    pub created_at: String,
    pub description: String,
    pub monthly_price_in_cents: u64,
    pub monthly_price_in_dollars: u64,
    pub name: String,
}

/// WatchEvent payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchEventPayload {
    pub action: String, // "started"
}

/// Common User structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: u64,
    pub login: String,
    pub gravatar_id: String,
    pub url: String,
    pub html_url: String,
    pub followers_url: String,
    pub following_url: String,
    pub gists_url: String,
    pub starred_url: String,
    pub subscriptions_url: String,
    pub organizations_url: String,
    pub repos_url: String,
    pub events_url: String,
    pub received_events_url: String,
    pub site_admin: bool,
    pub avatar_url: String,
    #[serde(rename = "type")]
    pub user_type: String,
}

/// Helper function to parse a GitHub event into a specific type
impl GitHubEvent {
    pub fn parse_payload<T>(&self) -> Result<T, serde_json::Error>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_value(self.payload.clone())
    }
}

/// Helper functions for common event type parsing
impl GitHubEvent {
    pub fn as_push_event(&self) -> Option<PushEventPayload> {
        if self.event_type == "PushEvent" {
            self.parse_payload().ok()
        } else {
            None
        }
    }

    pub fn as_pull_request_event(&self) -> Option<PullRequestEventPayload> {
        if self.event_type == "PullRequestEvent" {
            self.parse_payload().ok()
        } else {
            None
        }
    }

    pub fn as_issues_event(&self) -> Option<IssuesEventPayload> {
        if self.event_type == "IssuesEvent" {
            self.parse_payload().ok()
        } else {
            None
        }
    }

    pub fn as_fork_event(&self) -> Option<ForkEventPayload> {
        if self.event_type == "ForkEvent" {
            self.parse_payload().ok()
        } else {
            None
        }
    }

    pub fn as_watch_event(&self) -> Option<WatchEventPayload> {
        if self.event_type == "WatchEvent" {
            self.parse_payload().ok()
        } else {
            None
        }
    }

    pub fn as_create_event(&self) -> Option<CreateEventPayload> {
        if self.event_type == "CreateEvent" {
            self.parse_payload().ok()
        } else {
            None
        }
    }

    pub fn as_delete_event(&self) -> Option<DeleteEventPayload> {
        if self.event_type == "DeleteEvent" {
            self.parse_payload().ok()
        } else {
            None
        }
    }

    pub fn as_release_event(&self) -> Option<ReleaseEventPayload> {
        if self.event_type == "ReleaseEvent" {
            self.parse_payload().ok()
        } else {
            None
        }
    }

    pub fn as_issue_comment_event(&self) -> Option<IssueCommentEventPayload> {
        if self.event_type == "IssueCommentEvent" {
            let maybe_ok = self.parse_payload();
            if let Ok(payload) = maybe_ok {
                return Some(payload);
            }
            panic!("Failed to parse IssueCommentEvent: {:?}", maybe_ok);
        } else {
            None
        }
    }
}
