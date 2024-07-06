-- Snowflake dates dimension table, inspired by BigQuery implementation.
-- Run this daily, early in the morning. Note that you might not need all the columns.
-- Only compute what you really need and make this template yours.

/* Disclaimer: This script hasn't been tested on Snowflake */

with recursive date_series as (

    select 
        dateadd(day, seq4(), current_date())::date as d   -- Snowflake's recursive CTE, starts from current date
    from table(generator(rowcount => 
        datediff('day', current_date(), LAST_DAY(dateadd('month', 2, current_date()))) + 1 
    ))   -- Dynamic row count for rest of current month + 2 months for improved target visualizations

),

date_details as (

    -- Assuming your week starts on Monday
    
    select
        to_char(d, 'yyyy-mm-dd') as id,       -- Snowflake format
        d as full_date,
        date_trunc('week', d)::date as week_date,
        date_trunc('month', d)::date as month_date,
        date_trunc('quarter', d)::date as quarter_date,
        weekofyear(d, 1) as year_week,        -- Monday as week start
        dayofyear(d) as year_day,
        year(d) as fiscal_year,
        quarter(d) as fiscal_qtr,
        month(d) as month,
        to_char(d, 'Month') as month_name,    -- Snowflake format
        dayname(d) as day_name,              -- Snowflake function
        decode( -- Sorting column for the weekday column in Power BI
            dayname(d),
            'Monday', 1, 
            'Tuesday', 2,
            'Wednesday', 3,
            'Thursday', 4,
            'Friday', 5,
            'Saturday', 6,
            'Sunday', 7
        ) as day_name_order,
        dayname(d) not in ('Sunday', 'Saturday') as day_is_weekday,

        -- Custom quarter start date calculation (shifted by one month for improved planning cycles)
        case 
            when month(d) < 2 then date_from_parts(year(d) - 1, 11, 1)
            when month(d) < 5 then date_from_parts(year(d), 2, 1)
            when month(d) < 8 then date_from_parts(year(d), 5, 1)
            when month(d) < 11 then date_from_parts(year(d), 8, 1)
            else date_from_parts(year(d), 11, 1) 
        end as shifted_quarter_date

    from date_series
)

select
    dd.*,
    
    -- Last and next periods for simplified Power BI filtering
    dateadd('week', 1, week_date) as next_week_date,
    dateadd('quarter', -1, quarter_date) as last_quarter_date,
    dateadd('quarter', 1, quarter_date) as next_quarter_date,

    -- Similar logic as above for last/next shifted_quarter_date calculations
    case 
        when month(full_date) <= 4 then date_from_parts(year(full_date) - 1, 11, 1)
        when month(full_date) <= 7 then date_from_parts(year(full_date), 2, 1)
        when month(full_date) <= 10 then date_from_parts(year(full_date), 5, 1)
        else date_from_parts(year(full_date), 8, 1)
    end as last_shifted_quarter_date,
    case
        when month(full_date) = 1 then date_from_parts(year(full_date), 2, 1)
        when month(full_date) between 2 and 4 then date_from_parts(year(full_date), 5, 1)
        when month(full_date) between 5 and 7 then date_from_parts(year(full_date), 8, 1)
        when month(full_date) between 8 and 10 then date_from_parts(year(full_date), 11, 1)
        else date_from_parts(year(full_date) + 1, 2, 1)
    end as next_shifted_quarter_date,

    -- Public holidays, optional
    /*
        Here, we reference a table containing public holiday dates.
        If you use dbt, this might be a seed.
    */
    h.date is not null as is_holiday  -- LEFT JOIN here for holiday flag

from date_details dd
left join holidays h on dd.full_date = h.date  
order by id;
