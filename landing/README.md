# DataSync Landing Page

Landing page para DataSync - Plataforma de sincronización y gobernanza de datos.

## Desarrollo

```bash
npm install
npm run dev
```

## Build para producción

```bash
npm run build
```

El build estará en la carpeta `dist/` listo para desplegar a Netlify, Vercel, GitHub Pages, etc.

## Tecnologías

- React 18
- TypeScript
- Vite
- Styled Components

## Deploy

### Netlify

1. Conecta tu repositorio
2. Build command: `npm run build`
3. Publish directory: `dist`

### Vercel

1. Conecta tu repositorio
2. Framework preset: Vite
3. Build command: `npm run build`
4. Output directory: `dist`

### GitHub Pages

1. Build el proyecto: `npm run build`
2. Sube la carpeta `dist` a la rama `gh-pages` o usa GitHub Actions
