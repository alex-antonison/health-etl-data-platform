# Welcome to HealthETL's Data Platform

At HealthETL, we are focused around efficiently and securely ingesting client health monitoring data to help our patient population achieve their health goals.

## Overview

### Source Data

While the following is our current application model, we are committed to meeting our patients where they are at so as our patient's needs evolve, so will our application!

```text
# Base class
class AppResult:
    created_time: (datetime)
    modified_time: (datetime)
    content_slug: (string)
    patient: (pk)

# Concrete subclass
class DateTimeAppResult(AppResult):
    value: (datetime)

# Concrete subclass
class IntegerAppResult(AppResult):
    value: (integer)

# Concrete subclass
class RangeAppResult(AppResult):
    from: (integer)
    to: (integer)
```

### Data Platform Needs

#### Timeliness

 To ensure our patients and providers have access to the data they need, our data platform will be updated daily.

#### Data Governance TODO

To ensure only the appropriate people have access to the data to help our patients, we have defined the following roles:

|role|access|
|-|-|
|Software Developers|Access to development environments data only. No access to production PHI.|
|Data Engineers|Full access to data pipelines and infrastructure. Limited access to PHI for debugging purposes only.|
|Data Analysts|Read-only access to de-identified data for analysis. No direct access to PHI.|
|3rd parties responsible for extracting data from hospital EHRs|Temporary, time-bound access to specific EHR endpoints. Access revoked after data transfer completion.|
|Data scientists and ML engineers|Access to de-identified datasets for model development. No direct access to PHI.|
