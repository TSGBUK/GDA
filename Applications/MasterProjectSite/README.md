# MasterProjectSite

This folder is the **GitHub Pages publishing root** for the GDA project website.

## About

The site is a pure HTML/JS static website providing an overview of the Grid Data Analysis (GDA) project, including information about the services, projects, blogs, pricing, and contact details.

## GitHub Pages

The site is published via GitHub Pages at:

**https://tsgbuk.github.io/GDA/**

### Enabling GitHub Pages

To enable or re-enable GitHub Pages publishing from this folder:

1. Go to **Settings → Pages** in the GitHub repository.
2. Under **Build and deployment**, set the source to **Deploy from a branch**.
3. Select the **`main`** branch and the **`/Applications/MasterProjectSite`** folder.
4. Click **Save**.

GitHub will build and deploy the site automatically. See [`PAGES_SETUP.md`](../../PAGES_SETUP.md) at the repository root for full instructions and troubleshooting tips.

## Structure

| File | Description |
|---|---|
| `index.html` | Homepage / entry point |
| `about.html` | About page |
| `services.html` | Services overview |
| `projects.html` | Projects showcase |
| `blogs.html` | Blog listings |
| `pricing.html` | Pricing information |
| `contact.html` | Contact form/details |
| `privacy.html` | Privacy policy |
| `terms.html` | Terms of service |
| `src/` | Static assets (CSS, JS, images) |
| `.nojekyll` | Disables Jekyll processing on GitHub Pages |
