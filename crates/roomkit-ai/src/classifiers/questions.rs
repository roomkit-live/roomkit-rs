//! The three narrow question types of RFC §6.8.

use roomkit::ChannelError;
use serde::{Deserialize, Serialize};

use super::error::invalid;

crate::shared::ordered::ordered_mapping!(
    /// A choice's named options, in insertion order. Cloning shares this mapping.
    ChoiceOptions, String
);

/// Whether something holds, answered with the probability of yes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct YesNoQuestion {
    /// The question and what it means.
    pub instructions: String,
    /// What a yes covers, when it needs saying.
    pub yes: Option<String>,
    /// What a no covers.
    pub no: Option<String>,
}

impl YesNoQuestion {
    /// A question with no additional description of yes or no.
    pub fn new(instructions: impl Into<String>) -> Self {
        Self {
            instructions: instructions.into(),
            yes: None,
            no: None,
        }
    }
}

/// One option among several, answered with each option's probability.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChoiceQuestion {
    /// The question and what it means.
    pub instructions: String,
    /// The shared named options; construction requires at least two.
    pub options: ChoiceOptions,
}

impl ChoiceQuestion {
    /// Keeps the supplied options and refuses fewer than two, as Python does.
    ///
    /// # Errors
    /// `ValueError` when the supplied mapping has fewer than two options.
    pub fn new(
        instructions: impl Into<String>,
        options: ChoiceOptions,
    ) -> Result<Self, ChannelError> {
        if options.len() < 2 {
            return Err(invalid(
                "ValueError",
                "a ChoiceQuestion needs two options at least",
            ));
        }
        Ok(Self {
            instructions: instructions.into(),
            options,
        })
    }
}

/// A degree on an ordered scale, answered with its expected level.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScoreQuestion {
    /// The question and what it means.
    pub instructions: String,
    /// Descriptions from the lowest level to the highest.
    pub levels: Vec<String>,
}

impl ScoreQuestion {
    /// A scale with at least two levels.
    ///
    /// # Errors
    /// `ValueError` when the supplied scale has fewer than two levels.
    pub fn new(instructions: impl Into<String>, levels: Vec<String>) -> Result<Self, ChannelError> {
        if levels.len() < 2 {
            return Err(invalid(
                "ValueError",
                "a ScoreQuestion needs two levels at least",
            ));
        }
        Ok(Self {
            instructions: instructions.into(),
            levels,
        })
    }
}

/// A classifier's typed question.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Question {
    /// A choice among named options.
    Choice(ChoiceQuestion),
    /// An expected level on a scale.
    Score(ScoreQuestion),
    /// The probability of yes.
    YesNo(YesNoQuestion),
}

crate::shared::ordered::ordered_mapping!(
    /// Questions in the caller's order. A clone shares the outer mapping.
    Questions, Question
);
