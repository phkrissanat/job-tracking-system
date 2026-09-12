const CACHE_NAME = 'mold-tracker-v36';
const APP_SHELL = [
  './',
  './dashboard-admin.html',
  './readonly.html',
  './manifest.json',
  './manifest-readonly.json',
  './icon-192.png',
  './icon-512.png',
  './icon-maskable-512.png',
  './apple-touch-icon.png',
  './firebase-config.js',
  './firebase-init.js',
  './firebase-mold-sync.js'
];

// Install: pre-cache the app shell
self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL))
  );
});

// Activate: clean up old caches
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// Fetch: cache-first for app shell, network-first (with cache fallback) for everything else
self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  event.respondWith(
    caches.match(req).then((cached) => {
      const fetchPromise = fetch(req)
        .then((res) => {
          // status===200 covers same-origin files; cross-origin CDN requests
          // (xlsx.full.min.js, pdf.min.js, Google Fonts) come back as opaque
          // responses (status 0, type 'opaque') and must be cached too, or
          // they silently fail to load once the app is offline.
          if (res && (res.status === 200 || res.type === 'opaque')) {
            const resClone = res.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(req, resClone));
          }
          return res;
        })
        .catch(() => cached);
      return cached || fetchPromise;
    })
  );
});
