use crate::primitives::{RegularTimeDimension, TimeStep};
use crate::primitives::{TimeInstance, TimeInterval};
use std::iter::Peekable;

pub trait TimeFilledItem {
    fn create_fill_element(ti: TimeInterval) -> Self;
    fn time(&self) -> TimeInterval;
}

impl TimeFilledItem for TimeInterval {
    fn create_fill_element(ti: TimeInterval) -> Self {
        ti
    }

    fn time(&self) -> TimeInterval {
        // TODO: use Result here? Then, we coult impl TimeFilledItem for Option<TimeInterval> and Result<TimeInterval, E>...
        *self
    }
}

type TryTimeIrregularRangeFillType<T, E, S> = TryTimeGapFillIter<
    T,
    E,
    TryTimeGapFillIter<
        T,
        E,
        TryTimeGapFillIter<T, E, TryTimeGapFillIter<T, E, S, TimeSinglePrepend>, TimeGapSingleFill>,
        TimeSingleAppend,
    >,
    TimeEmptySingleFill,
>;

pub trait TryIrregularTimeFillIterExt<T: TimeFilledItem, E>: Iterator<Item = Result<T, E>>
where
    Self: Iterator<Item = Result<T, E>> + Sized,
{
    /// This creates an Iterator where time gaps between items are filled with a single tile inteval
    fn try_time_irregular_fill(self) -> TryTimeGapFillIter<T, E, Self, TimeGapSingleFill> {
        TryTimeGapFillIter::new(self, TimeGapSingleFill::new())
    }

    /// This creates an Iterator where a single fill item is prepended if the first item from the source starts after the specified start
    fn try_time_irregular_prepend(
        self,
        first_element_start_required: TimeInstance,
    ) -> TryTimeGapFillIter<T, E, Self, TimeSinglePrepend> {
        TryTimeGapFillIter::new(self, TimeSinglePrepend::new(first_element_start_required))
    }

    /// This creates an Iterator where a single fill item is appended if the last item from the source ends after the specified end
    fn try_time_irregular_append(
        self,
        last_element_end_required: TimeInstance,
    ) -> TryTimeGapFillIter<T, E, Self, TimeSingleAppend> {
        TryTimeGapFillIter::new(self, TimeSingleAppend::new(last_element_end_required))
    }

    /// This creates an Iterator where a single fill item is returned if the source is empty
    fn try_time_irregular_empty_fill(
        self,
        range: TimeInterval,
    ) -> TryTimeGapFillIter<T, E, Self, TimeEmptySingleFill> {
        TryTimeGapFillIter::new(self, TimeEmptySingleFill::new(range))
    }

    /// This creates an Iterator where:
    /// - IF the source is empty
    ///     - a single fill item is returned covering the source is empty
    /// - ELSE
    ///     - a single fill item is prepended if the first item from the source starts after the specified start
    ///     - time gaps between items are filled with a single tile inteval
    ///     - a single fill item is appended if the last item from the source ends after the specified end
    ///
    fn try_time_irregular_range_fill(
        self,
        range_to_cover: TimeInterval,
    ) -> TryTimeIrregularRangeFillType<T, E, Self> {
        self.try_time_irregular_prepend(range_to_cover.start())
            .try_time_irregular_fill()
            .try_time_irregular_append(range_to_cover.end())
            .try_time_irregular_empty_fill(range_to_cover)
    }
}

impl<T: TimeFilledItem, E, I: Iterator<Item = Result<T, E>>> TryIrregularTimeFillIterExt<T, E>
    for I
{
}

pub trait TimeGapFill {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction;
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

pub enum TimeGapFillNextAction {
    SourceNext,
    CreateFillElement(TimeInterval),
    End,
}

impl TimeGapFillNextAction {
    pub fn variant_name(&self) -> &'static str {
        match self {
            TimeGapFillNextAction::SourceNext => "SourceNext",
            TimeGapFillNextAction::CreateFillElement(_) => "CreateFillElement",
            TimeGapFillNextAction::End => "End",
        }
    }
}

pub struct TimeGapFillIter<T: TimeFilledItem, I: Iterator<Item = T>, S: TimeGapFill> {
    source: Peekable<I>,
    state: S,
}

impl<T: TimeFilledItem, I: Iterator<Item = T>, S: TimeGapFill> TimeGapFillIter<T, I, S> {
    pub fn new(source: I, state: S) -> Self {
        let peekable_source = Iterator::peekable(source);

        Self {
            source: peekable_source,
            state,
        }
    }
}

impl<T: TimeFilledItem, I: Iterator<Item = T>, S: TimeGapFill> Iterator
    for TimeGapFillIter<T, I, S>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let peek_time: Option<TimeInterval> = self.source.peek().map(T::time);
        let action = self.state.next_action(peek_time.as_ref());

        //dbg!(
        //    self.state.name(),
        //    action.variant_name(),
        //    next.as_ref().map(|t| t.time())
        //);
        match action {
            TimeGapFillNextAction::End => None,
            TimeGapFillNextAction::SourceNext => self.source.next(),
            TimeGapFillNextAction::CreateFillElement(ti) => Some(T::create_fill_element(ti)),
        }
    }
}

pub struct TryTimeGapFillIter<
    T: TimeFilledItem,
    E,
    I: Iterator<Item = Result<T, E>>,
    S: TimeGapFill,
> {
    source: Peekable<I>,
    state: S,
}

impl<T: TimeFilledItem, E, I: Iterator<Item = Result<T, E>>, S: TimeGapFill>
    TryTimeGapFillIter<T, E, I, S>
{
    pub fn new(source: I, state: S) -> Self {
        let peekable_source = Iterator::peekable(source);

        Self {
            source: peekable_source,
            state,
        }
    }
}

impl<T: TimeFilledItem, E, I: Iterator<Item = Result<T, E>>, S: TimeGapFill> Iterator
    for TryTimeGapFillIter<T, E, I, S>
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let peek_time = self
            .source
            .peek()
            .and_then(|r| r.as_ref().ok())
            .map(T::time);
        match self.state.next_action(peek_time.as_ref()) {
            TimeGapFillNextAction::End => None,
            TimeGapFillNextAction::SourceNext => self.source.next(),
            TimeGapFillNextAction::CreateFillElement(ti) => Some(Ok(T::create_fill_element(ti))),
        }
    }
}

pub struct TimeGapSingleFill {
    last_time_end: Option<TimeInstance>,
}

impl Default for TimeGapSingleFill {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeGapSingleFill {
    pub fn new() -> Self {
        Self {
            last_time_end: None,
        }
    }
}

impl TimeGapFill for TimeGapSingleFill {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction {
        match (self.last_time_end, source_peek) {
            // the source produced None in a previous next() call
            // the next element in the source is None. Nothing to fill here
            (None | Some(_), None) => {
                self.last_time_end = None;
                TimeGapFillNextAction::End
            }

            // the source produces an element that starts later then the prevous element ended
            (Some(last_end), Some(peek)) if last_end < peek.start() => {
                self.last_time_end = Some(peek.end());
                TimeGapFillNextAction::CreateFillElement(TimeInterval::new_unchecked(
                    last_end,
                    peek.start(),
                ))
            }
            // the next element aligns with the previous element
            // the source produces its first element
            (None | Some(_), Some(peek)) => {
                self.last_time_end = Some(peek.end());
                TimeGapFillNextAction::SourceNext
            }
        }
    }
}

pub struct TimeSinglePrepend {
    start: Option<TimeInstance>,
}

impl TimeSinglePrepend {
    pub fn new(first_element_start_required: TimeInstance) -> Self {
        Self {
            start: Some(first_element_start_required),
        }
    }
}

impl TimeGapFill for TimeSinglePrepend {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction {
        let nn = match (self.start, source_peek) {
            // default case where the start has already been checked
            (None, _) => TimeGapFillNextAction::SourceNext,
            // the source never produced an element, can't prepend an element here
            (Some(_start), None) => TimeGapFillNextAction::End,
            // the first element starts after the required start so a single fill element is inserted here
            (Some(start), Some(peek)) if start < peek.start() => {
                TimeGapFillNextAction::CreateFillElement(TimeInterval::new_unchecked(
                    start,
                    peek.start(),
                ))
            }
            // the first element starts bevore or equal to the requred first start
            (Some(_start), Some(_peek)) => TimeGapFillNextAction::SourceNext,
        };
        // we only need to check the first element, so we can set start to None here
        self.start = None;
        nn
    }
}

pub struct TimeSingleAppend {
    last_time_end: Option<TimeInstance>,
    req_end: TimeInstance,
}

impl TimeSingleAppend {
    pub fn new(last_element_end: TimeInstance) -> Self {
        Self {
            last_time_end: None,
            req_end: last_element_end,
        }
    }
}

impl TimeGapFill for TimeSingleAppend {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction {
        match (self.last_time_end, source_peek) {
            // the source produces an element
            (_, Some(peek)) => {
                self.last_time_end = Some(peek.end());
                TimeGapFillNextAction::SourceNext
            }

            // the next element in the source is None. Check if we need to append a fill element
            (Some(last_end), None) if last_end < self.req_end => {
                self.last_time_end = None;
                TimeGapFillNextAction::CreateFillElement(TimeInterval::new_unchecked(
                    last_end,
                    self.req_end,
                ))
            }
            _ => TimeGapFillNextAction::End,
        }
    }
}

pub struct TimeEmptySingleFill {
    range: TimeInterval,
    pristine: bool,
}

impl TimeEmptySingleFill {
    pub fn new(range: TimeInterval) -> Self {
        Self {
            range,
            pristine: true,
        }
    }
}

impl TimeGapFill for TimeEmptySingleFill {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction {
        match (self.pristine, source_peek) {
            (true, None) => {
                self.pristine = false;
                TimeGapFillNextAction::CreateFillElement(self.range)
            }
            (_, Some(_)) => {
                self.pristine = false;
                TimeGapFillNextAction::SourceNext
            }
            (false, None) => TimeGapFillNextAction::End,
        }
    }
}

type TimeGapFillRangeAdapterIterSourceType<T, I> = TimeGapFillIter<
    T,
    TimeGapFillIter<T, TimeGapFillIter<T, I, TimeSinglePrepend>, TimeGapSingleFill>,
    TimeSingleAppend,
>;

pub struct TimeGapFillRangeAdapterIter<T: TimeFilledItem, I: Iterator<Item = T>> {
    source: TimeGapFillRangeAdapterIterSourceType<T, I>,
    range_to_cover: TimeInterval,
    pristine: bool,
}

impl<T: TimeFilledItem, I: Iterator<Item = T>> Iterator for TimeGapFillRangeAdapterIter<T, I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let is_fist_call = self.pristine;
        self.pristine = false;

        let nn = self.source.next();

        if is_fist_call && nn.is_none() {
            Some(T::create_fill_element(self.range_to_cover))
        } else {
            nn
        }
    }
}

pub struct TimeGapRegularFill {
    step: TimeStep,
    last_time: Option<TimeInstance>,
    finished: bool,
}

impl TimeGapRegularFill {
    pub fn new(step: TimeStep) -> Self {
        Self {
            step,
            last_time: None,
            finished: false,
        }
    }
}

impl TimeGapFill for TimeGapRegularFill {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction {
        if self.finished {
            return TimeGapFillNextAction::End;
        }

        match (self.last_time, source_peek) {
            // case where no TI is availabe from source
            // no new element from source
            (Some(_) | None, None) => {
                self.finished = true;
                TimeGapFillNextAction::End
            }

            // the source produces an element that starts later then the previous element ended
            (Some(last), Some(peek)) if last < peek.start() => {
                let next_start = last;
                let next_end = (last + self.step).expect("TimeStep addition overflowed"); // TODO: check range overflow?
                self.last_time = Some(next_end);
                TimeGapFillNextAction::CreateFillElement(TimeInterval::new_unchecked(
                    next_start, next_end,
                ))
            }

            // first element from source
            // the next element aligns with the previous element
            (None | Some(_), Some(peek)) => {
                self.last_time = Some(peek.end());
                TimeGapFillNextAction::SourceNext
            }
        }
    }
}

pub struct TimeGapRegularPrepend {
    step: TimeStep,
    next_time: Option<TimeInstance>,
}

impl TimeGapRegularPrepend {
    pub fn new(step: TimeStep, first_element_start_required: TimeInstance) -> Self {
        Self {
            step,
            next_time: Some(first_element_start_required),
        }
    }
}

impl TimeGapFill for TimeGapRegularPrepend {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction {
        match (self.next_time, source_peek) {
            // default case where the source produces elements
            (None, _) => TimeGapFillNextAction::SourceNext,
            // the source never produced an element, can't prepend an element here
            (Some(_start), None) => TimeGapFillNextAction::End,
            // the first element starts after the required start so a single fill element is inserted here
            (Some(next_time), Some(peek)) if next_time < peek.start() => {
                let next_element = TimeInterval::new_unchecked(
                    next_time,
                    (next_time + self.step).expect("TimeStep addition overflowed"), // TODO: check range overflow?
                );
                self.next_time = Some(next_element.end()); // we only need to check the first element, so we
                TimeGapFillNextAction::CreateFillElement(next_element)
            }
            // the first element starts bevore or equal to the requred first start --> no prepend needed
            (Some(_start), Some(_peek)) => {
                self.next_time = None;
                TimeGapFillNextAction::SourceNext
            }
        }
    }
}

pub struct TimeGapRegularAppend {
    step: TimeStep,
    last_time: TimeInstance,
    last_seen: Option<TimeInstance>,
    finished: bool,
}

impl TimeGapRegularAppend {
    pub fn new(step: TimeStep, last_element_end_required: TimeInstance) -> Self {
        Self {
            step,
            last_time: last_element_end_required,
            last_seen: None,
            finished: false,
        }
    }
}

impl TimeGapFill for TimeGapRegularAppend {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction {
        if self.finished {
            return TimeGapFillNextAction::End;
        }

        match (self.last_seen, source_peek) {
            // the source produces an element
            (_, Some(peek)) => {
                self.last_seen = Some(peek.end());
                TimeGapFillNextAction::SourceNext
            }

            // the next element in the source is None. Check if we need to append a fill element
            (Some(last_seen), None) if last_seen < self.last_time => {
                let next_start = last_seen;
                let next_end = (last_seen + self.step).expect("TimeStep addition overflowed"); // TODO: check range overflow?

                self.last_seen = Some(next_end);
                TimeGapFillNextAction::CreateFillElement(TimeInterval::new_unchecked(
                    next_start, next_end,
                ))
            }
            _ => {
                self.finished = true;
                TimeGapFillNextAction::End
            }
        }
    }
}

pub struct TimeEmptyRegularFill {
    range: TimeInterval,
    step: TimeStep,
    last_time: Option<TimeInterval>,
    fuse: bool,
}

impl TimeEmptyRegularFill {
    pub fn new(regular_dimension: RegularTimeDimension, range: TimeInterval) -> Self {
        Self {
            range,
            step: regular_dimension.step,
            last_time: None, // TODO: check range overflow?
            fuse: false,
        }
    }
}

impl TimeGapFill for TimeEmptyRegularFill {
    fn next_action(&mut self, source_peek: Option<&TimeInterval>) -> TimeGapFillNextAction {
        if self.fuse {
            return TimeGapFillNextAction::SourceNext;
        }

        match (self.last_time, source_peek) {
            (None, None) if !self.fuse => {
                let next = TimeInterval::new_unchecked(
                    self.range.start(),
                    (self.range.start() + self.step).expect("TimeStep addition overflowed"), // TODO: check range overflow?
                );
                self.last_time = Some(next);
                TimeGapFillNextAction::CreateFillElement(next)
            }
            (Some(last), None) if last.end() < self.range.end() => {
                let next_start = last.end();
                let next_end = (next_start + self.step).expect("TimeStep addition overflowed"); // TODO: check range overflow?
                let next = TimeInterval::new_unchecked(next_start, next_end);
                self.last_time = Some(next);
                TimeGapFillNextAction::CreateFillElement(next)
            }
            (Some(_) | None, None) => TimeGapFillNextAction::End,
            (_, Some(_)) => {
                self.fuse = true;
                TimeGapFillNextAction::SourceNext
            }
        }
    }
}

/// Type alias for the complex nested `TryTimeGapFillIter` used in `try_time_regular_range_fill`
pub type TryTimeRegularRangeFillType<T, E, S> = TryTimeGapFillIter<
    T,
    E,
    TryTimeGapFillIter<
        T,
        E,
        TryTimeGapFillIter<
            T,
            E,
            TryTimeGapFillIter<T, E, S, TimeGapRegularPrepend>,
            TimeGapRegularFill,
        >,
        TimeGapRegularAppend,
    >,
    TimeEmptyRegularFill,
>;

pub trait TryRegularTimeFillIterExt<T: TimeFilledItem, E>: Iterator<Item = Result<T, E>>
where
    Self: Iterator<Item = Result<T, E>> + Sized,
{
    /// This creates an Iterator where time gaps between items are filled with a single tile inteval
    fn try_time_regular_fill(
        self,
        step: TimeStep,
    ) -> TryTimeGapFillIter<T, E, Self, TimeGapRegularFill> {
        TryTimeGapFillIter::new(self, TimeGapRegularFill::new(step))
    }

    /// This creates an Iterator where a single fill item is prepended if the first item from the source starts after the specified start
    fn try_time_regular_prepend(
        self,
        step: TimeStep,
        first_element_start_required: TimeInstance,
    ) -> TryTimeGapFillIter<T, E, Self, TimeGapRegularPrepend> {
        TryTimeGapFillIter::new(
            self,
            TimeGapRegularPrepend::new(step, first_element_start_required),
        )
    }

    /// This creates an Iterator where a single fill item is appended if the last item from the source ends after the specified end
    fn try_time_regular_append(
        self,
        step: TimeStep,
        last_element_end_required: TimeInstance,
    ) -> TryTimeGapFillIter<T, E, Self, TimeGapRegularAppend> {
        TryTimeGapFillIter::new(
            self,
            TimeGapRegularAppend::new(step, last_element_end_required),
        )
    }

    /// This creates an Iterator where empty sources are filled with regular timesteps
    fn try_time_regular_empty_fill(
        self,
        regular_dimension: RegularTimeDimension,
        range: TimeInterval,
    ) -> TryTimeGapFillIter<T, E, Self, TimeEmptyRegularFill> {
        TryTimeGapFillIter::new(self, TimeEmptyRegularFill::new(regular_dimension, range))
    }

    /// This creates an Iterator where:
    /// - gaps before the first and after the last element are filled with regular step to fill the specified range
    /// - gaps are filled with regular step intervals
    /// - IF the source is empty, regular steps are inserted anchored at the query start!
    fn try_time_regular_range_fill(
        self,
        regular_dimension: RegularTimeDimension,
        range_to_cover: TimeInterval,
    ) -> TryTimeRegularRangeFillType<T, E, Self> {
        self.try_time_regular_prepend(regular_dimension.step, range_to_cover.start())
            .try_time_regular_fill(regular_dimension.step)
            .try_time_regular_append(regular_dimension.step, range_to_cover.end())
            .try_time_regular_empty_fill(regular_dimension, range_to_cover)
    }
}

impl<T: TimeFilledItem, E, I: Iterator<Item = Result<T, E>>> TryRegularTimeFillIterExt<T, E> for I {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_source_intervals() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let iter = vec![]
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();
        assert_eq!(
            result.unwrap(),
            vec![TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(100).unwrap()
            )]
        );
    }

    #[test]
    fn test_source_covers_entire_range() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(0).unwrap(),
            TimeInstance::from_millis(100).unwrap(),
        )];
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(100).unwrap()
            )]
        );
    }

    #[test]
    fn test_source_starts_after_hint() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(20).unwrap(),
            TimeInstance::from_millis(40).unwrap(),
        )];
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(100).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_source_starts_before_hint() {
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(50).unwrap();
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(30).unwrap(),
                TimeInstance::from_millis(40).unwrap(),
            ),
        ];
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(50).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_source_with_gaps() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(40).unwrap(),
                TimeInstance::from_millis(60).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(80).unwrap(),
                TimeInstance::from_millis(90).unwrap(),
            ),
        ];
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(60).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(60).unwrap(),
                    TimeInstance::from_millis(80).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(80).unwrap(),
                    TimeInstance::from_millis(90).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(90).unwrap(),
                    TimeInstance::from_millis(100).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_source_ends_before_hint() {
        let start = TimeInstance::from_millis(0).unwrap();
        let end = TimeInstance::from_millis(100).unwrap();
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(40).unwrap(),
            ),
        ];
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(100).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_source_exactly_matches_hint() {
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(20).unwrap();
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(10).unwrap(),
            TimeInstance::from_millis(20).unwrap(),
        )];
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap()
            )]
        );
    }

    #[test]
    fn test_source_overlapping_start() {
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(50).unwrap();
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(30).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(40).unwrap(),
                TimeInstance::from_millis(60).unwrap(),
            ),
        ];
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(60).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_gap_fill_no_gaps() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(10).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(30).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_fill(step);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_gap_fill_with_gaps() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(10).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(30).unwrap(),
                TimeInstance::from_millis(40).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_fill(step);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_start_prepend() {
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(20).unwrap(),
            TimeInstance::from_millis(30).unwrap(),
        )];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(0).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_prepend(step, start);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_start_prepend_start_contained() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(10).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(5).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_prepend(step, start);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_start_prepend_no_action_1() {
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(0).unwrap(),
            TimeInstance::from_millis(10).unwrap(),
        )];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(0).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_prepend(step, start);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(10).unwrap()
            )]
        );
    }

    #[test]
    fn test_regular_time_start_prepend_no_action_2() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(0).unwrap(),
                TimeInstance::from_millis(10).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(0).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_prepend(step, start);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap(),
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap(),
                )
            ]
        );
    }

    #[test]
    fn test_regular_time_end_append() {
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(0).unwrap(),
            TimeInstance::from_millis(10).unwrap(),
        )];
        let step = TimeStep::millis(10);
        let end = TimeInstance::from_millis(30).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_append(step, end);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(0).unwrap(),
                    TimeInstance::from_millis(10).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_end_append_end_contained() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(30).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(30).unwrap(),
                TimeInstance::from_millis(40).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let end = TimeInstance::from_millis(35).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_append(step, end);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap(),
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap(),
                )
            ]
        );
    }

    #[test]
    fn test_regular_time_end_append_no_action_1() {
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(20).unwrap(),
            TimeInstance::from_millis(30).unwrap(),
        )];
        let step = TimeStep::millis(10);
        let end = TimeInstance::from_millis(30).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_append(step, end);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(30).unwrap()
            )]
        );
    }

    #[test]
    fn test_regular_time_end_append_no_action_2() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(30).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(30).unwrap(),
                TimeInstance::from_millis(40).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let end = TimeInstance::from_millis(40).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_append(step, end);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap(),
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap(),
                )
            ]
        );
    }

    #[test]
    fn test_regular_time_start_prepend_and_end_append() {
        let intervals = vec![TimeInterval::new_unchecked(
            TimeInstance::from_millis(20).unwrap(),
            TimeInstance::from_millis(30).unwrap(),
        )];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(40).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_prepend(step, start)
            .try_time_regular_append(step, end);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn test_regular_time_gap_fill_empty_source() {
        let intervals: Vec<TimeInterval> = vec![];
        let step = TimeStep::millis(10);
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_fill(step);
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();
        assert_eq!(result.unwrap(), vec![]);
    }

    #[test]
    fn time_regular_range_fill_no_gaps() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(10).unwrap(),
                TimeInstance::from_millis(20).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(30).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(30).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_range_fill(
                RegularTimeDimension::new(TimeInstance::from_millis(0).unwrap(), step),
                TimeInterval::new_unchecked(start, end),
            );
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn time_regular_all_cases() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(20).unwrap(),
                TimeInstance::from_millis(30).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(50).unwrap(),
                TimeInstance::from_millis(60).unwrap(),
            ),
        ];
        let step = TimeStep::millis(10);
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(70).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_regular_range_fill(
                RegularTimeDimension::new(TimeInstance::from_millis(0).unwrap(), step),
                TimeInterval::new_unchecked(start, end),
            );
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(20).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(20).unwrap(),
                    TimeInstance::from_millis(30).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(30).unwrap(),
                    TimeInstance::from_millis(40).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(40).unwrap(),
                    TimeInstance::from_millis(50).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(50).unwrap(),
                    TimeInstance::from_millis(60).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(60).unwrap(),
                    TimeInstance::from_millis(70).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn time_irregular_all_caes() {
        let intervals = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(15).unwrap(),
                TimeInstance::from_millis(25).unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_millis(45).unwrap(),
                TimeInstance::from_millis(55).unwrap(),
            ),
        ];
        let start = TimeInstance::from_millis(10).unwrap();
        let end = TimeInstance::from_millis(60).unwrap();
        let iter = intervals
            .into_iter()
            .map(|t| -> Result<TimeInterval, &str> { Ok(t) })
            .try_time_irregular_range_fill(TimeInterval::new_unchecked(start, end));
        let result: Result<Vec<TimeInterval>, _> = iter.collect::<Result<Vec<_>, _>>();

        assert_eq!(
            result.unwrap(),
            vec![
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(10).unwrap(),
                    TimeInstance::from_millis(15).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(15).unwrap(),
                    TimeInstance::from_millis(25).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(25).unwrap(),
                    TimeInstance::from_millis(45).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(45).unwrap(),
                    TimeInstance::from_millis(55).unwrap()
                ),
                TimeInterval::new_unchecked(
                    TimeInstance::from_millis(55).unwrap(),
                    TimeInstance::from_millis(60).unwrap()
                ),
            ]
        );
    }
}
