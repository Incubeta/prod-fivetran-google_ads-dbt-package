# Fivetran Google Ads dbt package

## What does this dbt package do?
* Materializes the Google Ads RAW_main tables using the data coming from the Google API.

## How do I use the dbt package?
### Step 1: Prerequisites
To use this dby package, you must have the following:
- At least one Fivetran Google connector syncing data for at least one of the predefined reports:
    - googleads_campaign_performance_v_1
    - googleads_keyword_performance_v_1
- A BigQuery data destination

### Step 2: Install the package
Include the following Google package version in your `packages.yml` file

### Step 3: Define input tables variables
This package reads the Google data from the different tables created by the Google Ads connector. 
The names of the tables can be changed by setting the correct name in the root `dbt_project.yml` file.

The following table shows the configuration keys and the default table names:

|key|default|
|---|-------|
|googleads_campaign_performance_v_1_identifier|googleads_campaign_performance_v_1|
|googleads_keyword_performance_v_1_identifier|googleads_keyword_performance_v_1|

If the connector uses different table names (for example campaign_performance_daily_report) this can be set in the `dbt_project.yml` as follows.

```yaml
vars:
    googleads_campaign_performance_v_1_test_identifier: googleads_campaign_performance_v_1_test
```

### (Optional) Step 4: Additional configurations

#### Disable reports:
Individual reports can be disabled in the `dbt_project.yml` file.

```yaml
vars:
    
```

#### Change output tables:
The following vars can be used to change the output table names:

|key| default                        |
|---|--------------------------------|
|googleads_campaign_performance_v1_alias| google-campaign_performance-v1 |
|googleads_keyword_performance_v1_alias| google-keyword_performance-v1  |


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
