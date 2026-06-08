-- normalise the stack overflow developer survey.
-- source: raw_stackoverflow_survey loaded from the csv in s3.
-- we keep only rows with a valid compensation figure.

with source as (
    select * from raw_stackoverflow_survey
),

cleaned as (
    select
        "ResponseId"                                    as response_id,
        "Country"                                       as country,
        "DevType"                                       as dev_type,
        "YearsCodePro"                                  as years_code_pro,
        "EdLevel"                                       as education_level,
        "RemoteWork"                                    as remote_work,
        -- languages / tools are semicolon-separated lists
        "LanguageHaveWorkedWith"                        as languages_used,
        "LanguageWantToWorkWith"                        as languages_wanted,
        "DatabaseHaveWorkedWith"                        as databases_used,
        "PlatformHaveWorkedWith"                        as platforms_used,
        try_cast("ConvertedCompYearly" as double)       as annual_comp_usd
    from source
    where
        try_cast("ConvertedCompYearly" as double) is not null
        and try_cast("ConvertedCompYearly" as double) between 10000 and 500000
)

select * from cleaned
