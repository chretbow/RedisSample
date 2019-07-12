namespace RedisSample.Applibs
{
    using System;

    public static class TimeStampHelper
    {
        public static long UtcNow
        {
            get
            {
                return UtcDateTimeToUtcTimeStamp(DateTime.UtcNow);
            }
        }

        public static DateTime ToLocalDateTime(long utcTimeStamp)
        {
            return GTM().AddMilliseconds(utcTimeStamp).ToLocalTime();
        }

        public static DateTime ToLocalDateTime(string datetimeString)
        {
            var datetime = DateTime.Parse(datetimeString);
            if (datetime.Kind == DateTimeKind.Unspecified)
            {
                datetime = DateTime.SpecifyKind(datetime, DateTimeKind.Local);
            }
            return datetime.ToLocalTime();
        }

        public static DateTime UnixTimeStampToLocalDateTime(long unixTimeStamp)
        {
            return GTM().AddSeconds(unixTimeStamp).ToLocalTime();
        }

        public static DateTime ToUtcDateTime(long utcTimeStamp)
        {
            return GTM().AddMilliseconds(utcTimeStamp);
        }

        public static long ToUtcTimeStamp(DateTime datetime)
        {
            return UtcDateTimeToUtcTimeStamp(datetime.ToUniversalTime());
        }

        public static long ToUtcTimeStamp(string datetimeString)
        {
            var datetime = DateTime.Parse(datetimeString);
            if (datetime.Kind == System.DateTimeKind.Unspecified)
            {
                datetime = DateTime.SpecifyKind(datetime, DateTimeKind.Local);
            }
            return ToUtcTimeStamp(datetime);
        }

        public static string ToCron7(DateTime dateTime)
        {
            // second: (range: 0-59)
            // minute: (range: 0-59)
            // hour: (range: 0-23)
            // day of month: (range: 1-31)(在使用時要注意那個月有幾天)
            // month: (range: 1-12)
            // day of week (range: 1-7, 1 standing for Monday)
            // year(optional): (range: 1900-3000)
            return System.String.Format("{0} {1} {2} {3} {4} {5} {6}"
                , "0"
                , dateTime.Minute
                , dateTime.Hour
                , dateTime.Day
                , dateTime.Month
                , "?"
                , dateTime.Year);
        }

        public static string ToCron5(DateTime dateTime)
        {
            // min: 每小時的第幾分鐘，範圍為 0-59
            // hour: 每天的第幾個小時，範圍為 0 - 23
            // day of month: 每個月的第幾天，範圍為 1 - 31。
            // month: 每年的第幾個月，範圍為 1 - 12。
            // day of week: 每星期的星期幾，範圍為 0 - 7，0 與 7 都是星期日，1 為星期一，2 為星期二，餘類推。
            return System.String.Format("{0} {1} {2} {3} {4}"
                , dateTime.Minute
                , dateTime.Hour
                , dateTime.Day
                , dateTime.Month
                , dateTime.DayOfWeek);
        }

        public static DateTime Truncate(DateTime dateTime, TimeSpan timeSpan)
        {
            if (timeSpan == TimeSpan.Zero) return dateTime; // Or could throw an ArgumentException
            return dateTime.AddTicks(-(dateTime.Ticks % timeSpan.Ticks));
        }

        private static DateTime GTM()
        {
            return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);    // GTM時間
        }

        private static long UtcDateTimeToUtcTimeStamp(DateTime utcDatetime)
        {
            if (utcDatetime.Kind != System.DateTimeKind.Utc)
                throw new ArgumentException($"UtcDateTimeToUtcTimeStamp, {utcDatetime}, utcDatetime.Kind({utcDatetime.Kind}) != System.DateTimeKind.Utc");

            DateTime gtm = GTM();
            return Convert.ToInt64(((TimeSpan)utcDatetime.Subtract(gtm)).TotalMilliseconds);
        }
    }
}
