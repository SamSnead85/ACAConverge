// Service Worker for ACA DataHub PWA
const CACHE_NAME = 'aca-datahub-v3';
const STATIC_CACHE = 'static-v3';
const DYNAMIC_CACHE = 'dynamic-v3';

// Static assets to cache
const STATIC_ASSETS = [
    '/',
    '/index.html',
    '/manifest.json',
    '/icons/icon-192.png',
    '/icons/icon-512.png'
];

// Install event - cache static assets
self.addEventListener('install', (event) => {
    console.log('[SW] Installing Service Worker');
    event.waitUntil(
        caches.open(STATIC_CACHE)
            .then((cache) => {
                console.log('[SW] Caching static assets');
                return cache.addAll(STATIC_ASSETS);
            })
            .then(() => self.skipWaiting())
    );
});

// Activate event - clean old caches
self.addEventListener('activate', (event) => {
    console.log('[SW] Activating Service Worker');
    event.waitUntil(
        caches.keys()
            .then((keys) => {
                return Promise.all(
                    keys
                        .filter((key) => key !== STATIC_CACHE && key !== DYNAMIC_CACHE)
                        .map((key) => {
                            console.log('[SW] Removing old cache:', key);
                            return caches.delete(key);
                        })
                );
            })
            .then(() => self.clients.claim())
    );
});

// Fetch event - serve from cache, fallback to network
self.addEventListener('fetch', (event) => {
    const { request } = event;
    const url = new URL(request.url);

    // Skip non-GET requests
    if (request.method !== 'GET') return;

    // Skip API requests (don't cache)
    if (url.pathname.startsWith('/api/')) {
        return;
    }

    // Skip WebSocket connections
    if (url.protocol === 'ws:' || url.protocol === 'wss:') {
        return;
    }

    event.respondWith(
        caches.match(request)
            .then((cachedResponse) => {
                if (cachedResponse) {
                    // Return cached response and update cache in background
                    event.waitUntil(updateCache(request));
                    return cachedResponse;
                }

                // Fetch from network
                return fetch(request)
                    .then((response) => {
                        // Cache successful responses
                        if (response.ok) {
                            const responseClone = response.clone();
                            caches.open(DYNAMIC_CACHE)
                                .then((cache) => cache.put(request, responseClone));
                        }
                        return response;
                    })
                    .catch(() => {
                        // Return offline page for navigation requests
                        if (request.mode === 'navigate') {
                            return caches.match('/offline.html');
                        }
                    });
            })
    );
});

// Update cache in background
async function updateCache(request) {
    try {
        const response = await fetch(request);
        if (response.ok) {
            const cache = await caches.open(DYNAMIC_CACHE);
            await cache.put(request, response);
        }
    } catch (error) {
        console.log('[SW] Background update failed:', error);
    }
}

// Push notification handling
self.addEventListener('push', (event) => {
    console.log('[SW] Push received');

    const options = {
        body: event.data?.text() || 'New notification',
        icon: '/icons/icon-192.png',
        badge: '/icons/badge-72.png',
        vibrate: [100, 50, 100],
        data: {
            dateOfArrival: Date.now(),
            primaryKey: 1
        },
        actions: [
            { action: 'view', title: 'View', icon: '/icons/view.png' },
            { action: 'dismiss', title: 'Dismiss', icon: '/icons/dismiss.png' }
        ]
    };

    event.waitUntil(
        self.registration.showNotification('ACA DataHub', options)
    );
});

// Notification click handling
self.addEventListener('notificationclick', (event) => {
    console.log('[SW] Notification clicked');
    event.notification.close();

    if (event.action === 'view') {
        event.waitUntil(
            clients.openWindow('/')
        );
    }
});

// Background sync for offline actions
self.addEventListener('sync', (event) => {
    console.log('[SW] Background sync:', event.tag);

    if (event.tag === 'sync-queries') {
        event.waitUntil(syncQueries());
    }
});

async function syncQueries() {
    // Sync queued queries when back online
    const db = await openDB();
    const queries = await db.getAll('pending-queries');

    for (const query of queries) {
        try {
            await fetch('/api/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(query)
            });
            await db.delete('pending-queries', query.id);
        } catch (error) {
            console.log('[SW] Query sync failed:', error);
        }
    }
}

// Simple IndexedDB wrapper
function openDB() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open('aca-datahub', 1);
        request.onerror = () => reject(request.error);
        request.onsuccess = () => resolve({
            getAll: (store) => new Promise((res) => {
                const tx = request.result.transaction(store, 'readonly');
                const req = tx.objectStore(store).getAll();
                req.onsuccess = () => res(req.result);
            }),
            delete: (store, key) => new Promise((res) => {
                const tx = request.result.transaction(store, 'readwrite');
                tx.objectStore(store).delete(key);
                tx.oncomplete = () => res();
            })
        });
        request.onupgradeneeded = () => {
            request.result.createObjectStore('pending-queries', { keyPath: 'id' });
        };
    });
}
