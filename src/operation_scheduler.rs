use std::{ops::Range, time::Duration};

use time::{Date, Time, UtcDateTime};

pub enum StartTime {
    Now,
    Later(UtcDateTime),
}

pub trait OperationScheduler {
    fn get_start_time(&self, bytes_to_upload: usize) -> StartTime;
}

pub struct AnyTime;
impl OperationScheduler for AnyTime {
    fn get_start_time(&self, _bytes_to_upload: usize) -> StartTime {
        StartTime::Now
    }
}

/// Does the operation at a time interval.
/// If there is no time interval that can fit the operation, the largest time interval is used
/// and the operation will continue running past the end of the largest time interval.
pub struct TimesOfDay {
    intervals: Box<[Range<Time>]>,
    upload_speed: f64,
}

impl TimesOfDay {
    /// Intervals must not overlap. Upload speed is in bytes per second.
    pub fn new(mut intervals: Box<[Range<Time>]>, upload_speed: f64) -> Self {
        if intervals.is_empty() {
            panic!("Must specify at least 1 interval");
        }
        intervals.sort_by_key(|range| range.start);
        Self {
            intervals,
            upload_speed,
        }
    }

    fn get_start_time(&self, now: UtcDateTime, duration: Duration) -> UtcDateTime {
        fn duration_between(start: Time, end: Time) -> time::Duration {
            if start <= end {
                end - start
            } else {
                // Goes past 12am
                UtcDateTime::new(Date::MIN.next_day().unwrap(), end)
                    - UtcDateTime::new(Date::MIN, start)
            }
        }

        if let Some(start_time_today) = self.intervals.iter().find_map(|interval| {
            let start = if now.time() > interval.start {
                if interval.end > interval.start {
                    if now.time() < interval.end {
                        Some(now.time())
                    } else {
                        None
                    }
                } else {
                    Some(now.time())
                }
            } else {
                Some(interval.start)
            }?;
            let available_duration = duration_between(start, interval.end);
            println!("Available duration: {available_duration:?}");
            if available_duration >= duration {
                Some(now.replace_time(start))
            } else {
                None
            }
        }) {
            println!("Today");
            return start_time_today;
        };
        if let Some(start_time_tomorrow) = self
            .intervals
            .iter()
            .filter(|range| duration_between(range.start, range.end) >= duration)
            .min_by_key(|range| range.start)
            .map(|range| UtcDateTime::new(now.date().next_day().unwrap(), range.start))
        {
            println!("Tomorrow");
            return start_time_tomorrow;
        };
        let longest_interval = self
            .intervals
            .iter()
            .max_by_key(|range| duration_between(range.start, range.end))
            .map(|range| {
                let date = if range.start > now.time() {
                    now.date()
                } else {
                    now.date().next_day().unwrap()
                };
                UtcDateTime::new(date, range.start)
            });
        longest_interval.unwrap()
    }
}

impl OperationScheduler for TimesOfDay {
    fn get_start_time(&self, bytes_to_upload: usize) -> StartTime {
        let now = UtcDateTime::now();
        let start = self.get_start_time(
            now,
            Duration::from_secs_f64(bytes_to_upload as f64 / self.upload_speed),
        );
        StartTime::Later(start)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use time::{Date, Time, UtcDateTime};

    use crate::TimesOfDay;

    #[test]
    fn later_at_night() {
        let time = TimesOfDay::new(
            Box::new([Time::from_hms(22, 0, 0).unwrap()..Time::from_hms(6, 0, 0).unwrap()]),
            5_000_000.0,
        )
        .get_start_time(
            UtcDateTime::new(Date::MIN, Time::from_hms(15, 0, 0).unwrap()),
            Duration::from_secs(60 * 60 * 2),
        );
        assert_eq!(
            time,
            UtcDateTime::new(Date::MIN, Time::from_hms(22, 0, 0).unwrap())
        );
    }

    #[test]
    fn now() {
        let time = TimesOfDay::new(
            Box::new([Time::from_hms(22, 0, 0).unwrap()..Time::from_hms(6, 0, 0).unwrap()]),
            5_000_000.0,
        )
        .get_start_time(
            UtcDateTime::new(Date::MIN, Time::from_hms(23, 0, 0).unwrap()),
            Duration::from_secs(60 * 60 * 2),
        );
        assert_eq!(
            time,
            UtcDateTime::new(Date::MIN, Time::from_hms(23, 0, 0).unwrap())
        );
    }

    #[test]
    fn tomorrow() {
        let time = TimesOfDay::new(
            Box::new([Time::from_hms(22, 0, 0).unwrap()..Time::from_hms(6, 0, 0).unwrap()]),
            5_000_000.0,
        )
        .get_start_time(
            UtcDateTime::new(Date::MIN, Time::from_hms(23, 0, 0).unwrap()),
            Duration::from_secs(60 * 60 * 8),
        );
        assert_eq!(
            time,
            UtcDateTime::new(
                Date::MIN.next_day().unwrap(),
                Time::from_hms(22, 0, 0).unwrap()
            )
        );
    }

    #[test]
    fn longest_interval() {
        let time = TimesOfDay::new(
            Box::new([
                Time::from_hms(12, 0, 0).unwrap()..Time::from_hms(13, 0, 0).unwrap(),
                Time::from_hms(22, 0, 0).unwrap()..Time::from_hms(6, 0, 0).unwrap(),
            ]),
            5_000_000.0,
        )
        .get_start_time(
            UtcDateTime::new(Date::MIN, Time::from_hms(10, 0, 0).unwrap()),
            Duration::from_secs(60 * 60 * 10),
        );
        assert_eq!(
            time,
            UtcDateTime::new(Date::MIN, Time::from_hms(22, 0, 0).unwrap())
        );
    }
}
