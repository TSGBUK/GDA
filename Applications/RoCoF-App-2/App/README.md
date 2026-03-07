# RoCoF-App-2 App Route

This folder is the editable v2 app surface for RoCoF.

- `DashboardTemplateTheme/` remains the untouched vendor/template source.
- `App/` contains copied assets plus GDA-specific pages.
- Current base pages:
  - `index.html`
  - `datasets.html`
  - `dataset-view.html`

## Notes

- Navigation is generated from workspace dataset metadata in `assets/js/gda-v2.js`.
- Dataset pages are currently generic placeholders, ready for API-backed table views.
- Styling is based on copied template assets (`assets/css/output.css`) plus `assets/css/gda-v2.css`.
