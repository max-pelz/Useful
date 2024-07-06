/*
    BigQuery dates dimension table that comes in handy for e.g. Power BI implementation.
    Run this daily, early in the morning. Note that you might not need all the columns.
    Only compute what you really need and make this template yours.
*/

with 

date_series as (

    select *
    from
        unnest(
            generate_date_array(
                '2021-01-01', -- Start date
                date_sub(
                    -- End date including some future dates for improved target visualizations
                    date_add(date_trunc(current_date(), month), interval 2 month),
                    interval 1 day
                ),
                interval 1 day
            )
        ) as d

),

date_details as (

    -- Assuming your week starts on Monday

    select
        format_date('%F', d) as id,
        d as full_date,
        date_trunc(d, week (monday)) as week_date,
        date_trunc(d, month) as month_date,
        date_trunc(d, quarter) as quarter_date,
        extract(week (monday) from d) as year_week,
        extract(day from d) as year_day,
        extract(year from d) as fiscal_year,
        extract(quarter from d) as fiscal_qtr,
        extract(month from d) as month,
        format_date('%B', d) as month_name,
        format_date('%A', d) as day_name,
        case -- Sorting column for the weekday column in Power BI
            when format_date('%A', d) = 'Monday' then 1
            when format_date('%A', d) = 'Tuesday' then 2
            when format_date('%A', d) = 'Wednesday' then 3
            when format_date('%A', d) = 'Thursday' then 4
            when format_date('%A', d) = 'Friday' then 5
            when format_date('%A', d) = 'Saturday' then 6
            when format_date('%A', d) = 'Sunday' then 7
        end as day_name_order,
        not (format_date('%A', d) in ('Sunday', 'Saturday')) as day_is_weekday,
        
        -- Custom quarter start date calculation (shifted by one month for improved planning cycles)
        case
            when extract(month from d) < 2 then date(extract(year from d) - 1, 11, 1)
            when extract(month from d) < 5 then date(extract(year from d), 2, 1)
            when extract(month from d) < 8 then date(extract(year from d), 5, 1)
            when extract(month from d) < 11 then date(extract(year from d), 8, 1)
            else date(extract(year from d), 11, 1)
        end as shifted_quarter_date,

        -- Public holidays, optional
        holidays.date is not null as is_holiday
    from date_series

    /*
        Here, we reference an Excel Worksheet we ingested via MS Fabric with dbt syntax.
        If you use dbt, this might be a seed. Note that we adhere to one set of public holidays.
        Should you need to implement different sets of holidays for different locations, this
        becomes more complex.
    */
    left join {{ source('src_excel__public_holidays', 'public_holidays') }} as holidays
        on date(split(holidays.date, ' ')[offset(0)]) = date_series.d

)

select
    *,

    -- Last and next periods for simplified Power BI filtering
    date_add(week_date, interval 1 week) as next_week_date,
    date_sub(quarter_date, interval 1 quarter) as last_quarter_date,
    date_add(quarter_date, interval 1 quarter) as next_quarter_date,
    case
        when extract(month from full_date) <= 4 then date(extract(year from full_date) - 1, 11, 1)  -- Before May, Q1, previous Q4
        when extract(month from full_date) <= 7 then date(extract(year from full_date), 2, 1)      -- Before August, Q2, current Q1
        when extract(month from full_date) <= 10 then date(extract(year from full_date), 5, 1)    -- Before November, Q3, current Q2
        else date(extract(year from full_date), 8, 1)                                           -- November and onwards, Q4, current Q3
    end as last_shifted_quarter_date,
    case
        when extract(month from full_date) between 1 and 1 then date(extract(year from full_date), 2, 1)
        when extract(month from full_date) between 2 and 4 then date(extract(year from full_date), 5, 1)
        when extract(month from full_date) between 5 and 7 then date(extract(year from full_date), 8, 1)
        when extract(month from full_date) between 8 and 10 then date(extract(year from full_date), 11, 1)
        else date(extract(year from full_date) + 1, 2, 1)
    end as next_shifted_quarter_date

from date_details
order by id
