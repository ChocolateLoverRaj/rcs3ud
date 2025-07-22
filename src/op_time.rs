use std::{future, ops::Range, time::Duration};

use time::{Date, Time, UtcDateTime};

pub trait OpTime {
    fn wait_until(&self, duration: Duration) -> Box<dyn Future<Output = ()>>;
}

pub struct AnyTime;
impl OpTime for AnyTime {
    fn wait_until(&self, _duration: Duration) -> Box<dyn Future<Output = ()>> {
        Box::new(future::ready(()))
    }
}

/// Does the operation at a time interval.
/// If there is no time interval that can fit the operation, the largest time interval is used
/// and the operation will continue running past the end of the largest time interval.
pub struct TimesOfDay {
    intervals: Box<[Range<Time>]>,
}

impl TimesOfDay {
    /// Intervals must not overlap
    pub fn new(intervals: impl Iterator<Item = Range<Time>>) -> Self {
        let mut intervals = intervals.collect::<Box<_>>();
        intervals.sort_by_key(|range| range.start);
        Self { intervals }
    }
}

impl OpTime for TimesOfDay {
    fn wait_until(&self, duration: Duration) -> Box<dyn Future<Output = ()>> {
        fn duration_between(start: Time, end: Time) -> time::Duration {
            if start <= end {
                end - start
            } else {
                // Goes past 12am
                UtcDateTime::new(Date::MIN.next_day().unwrap(), end)
                    - UtcDateTime::new(Date::MIN, start)
            }
        }

        let now = UtcDateTime::now();
        let start_time_today = self.intervals.iter().find_map(|interval| {
            let start = interval.start.max(now.time());
            let available_duration = duration_between(start, interval.end);
            if available_duration >= duration {
                Some(now.replace_time(start))
            } else {
                None
            }
        });
        let start_time_tomorrow = self
            .intervals
            .iter()
            .filter(|range| duration_between(range.start, range.end) > duration)
            .find();
        todo!()
    }
}
