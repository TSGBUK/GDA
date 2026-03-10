# GitHub Pages Setup Guide

This document explains how to publish the GDA project website from the `Applications/MasterProjectSite` folder using GitHub Pages.

---

## Overview

The static site lives in:

```
Applications/MasterProjectSite/
```

It is a pure HTML/JS site with no server-side processing required. The entry point is `index.html`.

The site is (or will be) published at:

> **https://tsgbuk.github.io/GDA/**

---

## Step-by-Step: Enabling GitHub Pages

### Step 1 — Navigate to Settings

In the GitHub repository ([TSGBUK/GDA](https://github.com/TSGBUK/GDA)), click the **Settings** tab at the top of the page.

### Step 2 — Open the Pages Section

In the left-hand sidebar, scroll down to **Code and automation** and click **Pages**.

### Step 3 — Configure the Source

Under **Build and deployment**:

1. Set **Source** to `Deploy from a branch`.
2. Set **Branch** to `main`.
3. Set **Folder** to `/Applications/MasterProjectSite`.
4. Click **Save**.

### Step 4 — Wait for Deployment

GitHub will run an automated Pages deployment. This usually takes **1–2 minutes**. You can monitor progress under the **Actions** tab — look for a workflow called *pages build and deployment*.

### Step 5 — Visit Your Site

Once the deployment has completed, your site will be live at:

**https://tsgbuk.github.io/GDA/**

---

## Repository File Notes

| File | Purpose |
|---|---|
| `Applications/MasterProjectSite/.nojekyll` | Tells GitHub Pages **not** to run Jekyll. Required because some files/folders start with underscores and the site is pure HTML/JS. |
| `Applications/MasterProjectSite/index.html` | The homepage / entry point that GitHub Pages serves by default. |

---

## Troubleshooting

### Site shows a 404 error

- Make sure the Pages source is set to `main` branch and the `/Applications/MasterProjectSite` folder (not the root `/`).
- Confirm that `index.html` exists in `Applications/MasterProjectSite/`.
- After changing the source folder, wait 1–2 minutes for the deployment to complete.
- Clear your browser cache and try again.

### Styles or scripts are not loading (blank/broken page)

- GitHub Pages URLs are **case-sensitive**. Check that all file references in HTML (`src=`, `href=`) match the exact filename casing on disk.
  - ✅ `src/css/style.css`
  - ❌ `src/CSS/Style.css`
- Verify that all referenced asset files are committed to the repository.

### Changes are not reflected after a push

- Go to **Actions** and check that the latest *pages build and deployment* run has completed successfully.
- If the workflow failed, click on it to read the error log.
- If no workflow runs appeared, double-check that Pages is still enabled in **Settings → Pages**.

### Jekyll interfering with assets (folders/files starting with `_`)

- The `.nojekyll` file in `Applications/MasterProjectSite/` disables Jekyll. If you accidentally delete it, GitHub Pages will skip files and folders whose names begin with an underscore.
- To restore it, create an empty file at `Applications/MasterProjectSite/.nojekyll` and commit it.

### Custom domain

If you later want to use a custom domain (e.g. `www.example.com`):

1. In **Settings → Pages**, enter your custom domain under **Custom domain** and click **Save**.
2. Add the required DNS records with your domain provider (GitHub will show you what records to add).
3. A `CNAME` file will be automatically committed to the Pages source folder.

---

## Reference Links

- [GitHub Pages documentation](https://docs.github.com/en/pages)
- [Configuring a publishing source](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site)
- [Troubleshooting 404 errors](https://docs.github.com/en/pages/getting-started-with-github-pages/troubleshooting-404-errors-for-github-pages-sites)
- [About custom domains](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site)
