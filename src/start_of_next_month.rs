use time::Date;

pub trait StartOfNextMonthExt {
    fn start_of_next_month(self) -> Self;
}

impl StartOfNextMonthExt for Date {
    fn start_of_next_month(mut self) -> Self {
        for _ in self.day()..=self.month().length(self.year()) {
            self = self.next_day().unwrap();
        }
        self
    }
}

#[cfg(test)]
pub mod tests {
    use time::{Date, Month};

    use crate::StartOfNextMonthExt;

    #[test]
    fn december() {
        assert_eq!(
            Date::from_calendar_date(2025, Month::December, 13)
                .unwrap()
                .start_of_next_month(),
            Date::from_calendar_date(2026, Month::January, 1).unwrap(),
        );
    }

    #[test]
    fn july() {
        assert_eq!(
            Date::from_calendar_date(2025, Month::July, 2)
                .unwrap()
                .start_of_next_month(),
            Date::from_calendar_date(2025, Month::August, 1).unwrap(),
        );
    }
}
