# Data Ingestion Activities within the Flo Platform Component Ecosystem

This README provides details to start development on an Ingest-aligned component within an *Azure-Native* environment.

## Table of contents
- [Data Ingestion Activities within the Flo Platform Component Ecosystem](#data-ingestion-activities-within-the-flo-platform-component-ecosystem)
  - [Table of contents](#table-of-contents)
  - [1) Building Components using the Component Building Playbook](#1-building-components-using-the-component-building-playbook)
  - [2) Accessing Azure-Specific Services](#2-accessing-azure-specific-services)
    - [2.1) KeyVault-Backed DataBricks Secret Scopes](#21-keyvault-backed-databricks-secret-scopes)
  - [3) Further SDK Support](#3-further-sdk-support)

## 1) Building Components using the Component Building Playbook

As an authoritative source of truth, the [Component Building Playbook](https://effective-adventure-oz454j3.pages.github.io/Component-Building/) provides a comprehensive guide to building components within the Flo Platform ecosystem. This playbook is a living document that is continuously updated to reflect the latest practices and guidelines for component development.

The Playbook can be accessed from the following link: [Component Building Playbook](https://effective-adventure-oz454j3.pages.github.io/Component-Building/)

## 2) Accessing Azure-Specific Services

For a high-level overview of the services within the Azure-Native development environment provisioned for you, please consult the [Azure SLZ Environment Overview](https://docs.google.com/document/d/1oIgftxQix3x8f34zAPw0fcE2fMBNUBMdBCFBfKdXH1M/edit?tab=t.pctegxo9in7j) document.


### 2.1) KeyVault-Backed DataBricks Secret Scopes
The following secret scopes are available within each DataBricks workspace that is provisioned for you. These scopes allow you to securely access sensitive credentials stored within the associated environment's KeyVault.

| **Environment** | **Databricks Secret Scope** | **Associated KeyVault** |
| :-------------: | :-------------------------: | :---------------------: |
{% if target_maturity == 'POC' -%}| Poc             | `<secret-scope>`            | [keyvault-name](keyvault-uri)|{%- endif %}
{% if target_maturity == 'Delivery' -%}| Dev             | `<secret-scope>`            | [keyvault-name](keyvault-uri)|{%- endif %}
{% if target_maturity == 'Delivery' -%}| Stage           | `<secret-scope>`            | [keyvault-name](keyvault-uri)|{%- endif %}
{% if target_maturity == 'Delivery' -%}| Prod           | `<secret-scope>`            | [keyvault-name](keyvault-uri)|{%- endif %}

An example of using this secret scope to retrieve a credential in a Python script includes:

```python
token = dbutils.secrets.get("client-proposition-dev-kv", "databricks-token")
```

Further documentation and guides can be found [here](https://docs.databricks.com/dev-tools/databricks-utils.html#secrets-utility-dbutilssecrets).

## 3) Further SDK Support

If a question or concern you have is not addressed after consulting the above documentation and playbooks provided, please reach out to the Flo Platform team over the following channels:

| Category | Supporting entity | Contact channel |
| :-----   | :-------------    | :-------------  |
| General support - Service Desk | Flo Platform team   |  [EAI General Service Request](https://explore-ai.atlassian.net/servicedesk/customer/portal/2/group/25)  |
| General support - Email | Flo Platform team   |  flo-platform-support@sandtech.com  |
| Ingest SDK issues | Cross-Domain-Component squad | kroos@sandtech.com, twhitehead@sandtech.com |
