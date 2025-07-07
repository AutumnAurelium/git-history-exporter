use serde::{Deserialize, Serialize};

use crate::gh::{IssueComment, Label, Milestone, PullRequest, PushEventPayload, User};

#[derive(Debug, Serialize, Deserialize)]
pub struct TrackedPullRequest {
    pub archive_data: PullRequest,
    pub events: Vec<TrackedEvent>,
}

impl TrackedPullRequest {
    pub fn from(pr_obj: PullRequest) -> Self {
        Self {
            archive_data: pr_obj,
            events: Vec::new()
        }
    }

    pub fn update_from(&mut self, pr_obj: PullRequest) {
        self.archive_data = pr_obj;
    }

    pub fn accept_comment_edit(&mut self, comment: IssueComment) {
        let event = self.events.iter_mut().find(|event| {
            if let TrackedEvent::Comment(comment_event) = event {
                comment_event.comment.id == comment.id
            } else {
                false
            }
        });

        if let Some(event) = event {
            if let TrackedEvent::Comment(comment_event) = event {
                comment_event.comment = comment;
            }
        } else {
            self.events.push(TrackedEvent::from_comment(comment));
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TrackedEvent {
    Comment(CommentEvent),
    Push(PushEvent),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommentEvent {
    pub comment: IssueComment,
}

impl TrackedEvent {
    pub fn from_comment(comment: IssueComment) -> Self {
        Self::Comment(CommentEvent { comment })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PushEvent {
    pub push: PushEventPayload
}

