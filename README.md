# tap-crossbeam

This is a [Singer](https://www.singer.io/) tap that produces JSON-formatted data following the Singer spec.

See the getting [started guide for running taps.](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-singer-with-python)

This tap:

- Pulls raw data from the [Crossbeam API](https://developers.crossbeam.com/)
- Extracts the following resources:
  - [partner_populations](https://developers.crossbeam.com/#53c31e87-71ed-4712-85b5-65877d0c0a0f)
  - [partners](https://developers.crossbeam.com/#dd64387a-b410-40f3-9993-e87f1df96963)
  - [populations](https://developers.crossbeam.com/#9287d593-395e-40cb-ae3b-f4651e01d366)
  - [reports](https://developers.crossbeam.com/#8dd228d3-2df4-4473-b6f6-3ac3c54e7d4b)
  - [reports_data](https://developers.crossbeam.com/#42c54267-9f96-4379-b1ff-720bdf96a47e)
  - [threads](https://developers.crossbeam.com/#4ab89b70-2b52-4405-a625-eeb09c0e7cef)
  - [thread_timelines](https://developers.crossbeam.com/#6315ece6-1805-4132-9337-13bf4607e77a)

### Authentication

Follow the [OAuth steps](https://developers.crossbeam.com/#authentication) to create a client_id, client_secret, and refresh_token

### Config File

```json
{
  "organization_uuid": <ORGANIZATION_UUID>,
  "client_id": <CLIENT_ID>,
  "client_secret": <CLIENT_SECRET>,
  "refresh_token": <REFRESH_TOKEN>
}
```

---

Copyright &copy; 2021 Stitch
