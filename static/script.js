document.addEventListener('DOMContentLoaded', () => {
    // Initialize map
    const map = L.map('map').setView([43.04, -88.55], 7);

    map.createPane('radarPane');
    map.getPane('radarPane').style.zIndex = '320';
    map.createPane('hazardPane');
    map.getPane('hazardPane').style.zIndex = '650';
    
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; CartoDB'
    }).addTo(map);

    let radarLayer = null;
    let hazardLayer = L.layerGroup().addTo(map);
    let radarMarker = null;
    let userMarker = null;
    const sidebar = document.querySelector('.sidebar');
    const uiToggle = document.getElementById('ui-toggle');
    const productSelect = document.getElementById('product-select');

    const PRODUCT_LEGEND = {
        N0Q: {
            title: 'Reflectivity (dBZ)',
            labels: ['-10', '10', '30', '50', '70'],
            gradient: 'linear-gradient(to right, #00ECEC, #01A000, #E70000, #FF00FF)',
        },
        N0U: {
            title: 'Velocity (kt)',
            labels: ['-45', '-20', '0', '20', '45'],
            gradient: 'linear-gradient(to right, #1d4ed8, #93c5fd, #f8fafc, #fca5a5, #b91c1c)',
        },
        N0C: {
            title: 'Correlation Coeff',
            labels: ['0.70', '0.80', '0.90', '0.95', '1.00'],
            gradient: 'linear-gradient(to right, #7f1d1d, #dc2626, #f59e0b, #84cc16, #16a34a)',
        },
    };

    function getStationInput() {
        return document.getElementById('station-id').value.toUpperCase().trim() || 'KMKX';
    }

    function getSelectedFile() {
        return (document.getElementById('file-select').value || '').trim();
    }

    function updateLegend(product) {
        const cfg = PRODUCT_LEGEND[product] || PRODUCT_LEGEND.N0Q;
        const titleEl = document.querySelector('#radar-legend .legend-title');
        const labelsEl = document.querySelector('#radar-legend .legend-labels');
        const barEl = document.querySelector('#radar-legend .legend-bar');

        if (titleEl) titleEl.textContent = cfg.title;
        if (barEl) barEl.style.background = cfg.gradient;
        if (labelsEl) {
            labelsEl.innerHTML = cfg.labels.map((v) => `<span>${v}</span>`).join('');
        }
    }

    async function loadLocalFiles(station, preferredFile = '') {
        const fileSelect = document.getElementById('file-select');
        const res = await fetch(`/api/files/${station}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);

        const desired = preferredFile || fileSelect.value || '';
        fileSelect.innerHTML = '';

        const latestOpt = document.createElement('option');
        latestOpt.value = '';
        latestOpt.textContent = 'Latest local file';
        fileSelect.appendChild(latestOpt);

        data.files.forEach((item) => {
            const opt = document.createElement('option');
            opt.value = item.file;
            opt.textContent = item.file;
            fileSelect.appendChild(opt);
        });

        const exists = Array.from(fileSelect.options).some((o) => o.value === desired);
        fileSelect.value = exists ? desired : '';
    }

    function clearHazards() {
        hazardLayer.clearLayers();
        const hazardList = document.getElementById('hazard-list');
        hazardList.innerHTML = '<p class="empty-msg">No hazards detected.</p>';
    }

    function labelForHazard(kind) {
        if (kind === 'tornado_signature') return 'Tornado Signature';
        if (kind === 'severe_storm') return 'Severe Storm';
        if (kind === 'water_hazard') return 'Water Hazard';
        return kind;
    }

    function renderHazards(boxes) {
        const hazardList = document.getElementById('hazard-list');
        hazardLayer.clearLayers();

        if (!boxes || boxes.length === 0) {
            hazardList.innerHTML = '<p class="empty-msg">No hazards detected.</p>';
            return;
        }

        hazardList.innerHTML = '';
        boxes.forEach((box, idx) => {
            const rect = L.rectangle(box.bounds, {
                pane: 'hazardPane',
                color: box.color || '#f97316',
                weight: 2,
                fillOpacity: 0.08,
                interactive: true,
            }).addTo(hazardLayer);

            const popup = [
                `<b>${labelForHazard(box.hazard_type)}</b>`,
                `Center: ${box.center[0]}, ${box.center[1]}`,
                `Size: ${box.dimensions_km.width} km x ${box.dimensions_km.height} km`,
                `DBZ p90: ${box.metrics.reflectivity_p90}`,
                `Vel p90: ${box.metrics.velocity_abs_p90}`,
                `CC p10: ${box.metrics.cc_p10}`,
            ].join('<br>');
            rect.bindPopup(popup);

            const row = document.createElement('div');
            row.className = 'hazard-item';
            row.innerHTML = `
                <div class="hazard-item-title">${idx + 1}. ${labelForHazard(box.hazard_type)}</div>
                <div class="hazard-item-sub">Center: ${box.center[0]}, ${box.center[1]}</div>
                <div class="hazard-item-sub">Box: ${box.dimensions_km.width} km x ${box.dimensions_km.height} km</div>
            `;
            row.addEventListener('click', () => {
                map.fitBounds(box.bounds, { maxZoom: 10 });
                rect.openPopup();
            });
            hazardList.appendChild(row);
        });
    }

    async function loadHazards(station, product, selectedFile) {
        let kind = 'all';
        if (product === 'N0Q') kind = 'water_hazard';
        if (product === 'N0U') kind = 'severe_storm';
        if (product === 'N0C') kind = 'tornado_signature';

        const params = new URLSearchParams({ kind });
        if (selectedFile) params.set('file', selectedFile);
        const hzRes = await fetch(`/api/hazards/${station}?${params.toString()}`);
        const hz = await hzRes.json();
        if (hz.error) throw new Error(hz.error);
        renderHazards(hz.boxes);
    }

    function setRadarMarker(meta) {
        if (!meta.center) return;
        if (radarMarker) map.removeLayer(radarMarker);
        radarMarker = L.marker(meta.center, {
            icon: L.divIcon({
                className: 'radar-pin',
                html: '<span></span>',
                iconSize: [12, 12],
                iconAnchor: [6, 6],
            }),
        }).addTo(map);
        radarMarker.bindPopup(`Radar Tower<br>${meta.center[0].toFixed(4)}, ${meta.center[1].toFixed(4)}`);
    }

    async function renderFromLocal(station, product, status, loader, selectedFile) {
        const fileLabel = selectedFile || 'latest local file';
        status.innerText = `Rendering ${fileLabel} for ${station}...`;

        const metaParams = new URLSearchParams();
        if (selectedFile) metaParams.set('file', selectedFile);
        const metaRes = await fetch(`/api/metadata/${station}?${metaParams.toString()}`);
        const meta = await metaRes.json();
        if (meta.error) throw new Error(meta.error);

        const renderParams = new URLSearchParams({ t: String(Date.now()) });
        if (selectedFile) renderParams.set('file', selectedFile);
        const imageUrl = `/api/render/${station}/${product}?${renderParams.toString()}`;
        if (radarLayer) map.removeLayer(radarLayer);

        radarLayer = L.imageOverlay(imageUrl, meta.bounds, {
            opacity: Number(document.getElementById('opacity-slider').value || 0.8),
            pane: 'radarPane',
            zIndex: 1000,
        }).addTo(map);

        map.fitBounds(meta.bounds);
        setRadarMarker(meta);
        await loadHazards(station, product, selectedFile);

        hazardLayer.eachLayer((layer) => {
            if (layer.bringToFront) layer.bringToFront();
        });

        status.innerText = `Station: ${station} | ${meta.time}`;
        loader.classList.add('hidden');
    }

    async function fetchRadar(downloadFirst) {
        const station = getStationInput();
        const product = productSelect.value;
        const selectedFile = getSelectedFile();
        const loader = document.getElementById('map-loader');
        const status = document.getElementById('file-info');

        updateLegend(product);

        loader.classList.remove('hidden');
        clearHazards();
        status.innerText = downloadFirst
            ? `Downloading latest ${station} .ar2v...`
            : `Loading latest local ${station} .ar2v...`;

        try {
            if (downloadFirst) {
                const dlRes = await fetch(`/api/download/${station}`);
                const dl = await dlRes.json();
                if (dl.error) throw new Error(dl.error);
                status.innerText = `Rendering ${dl.file}...`;
                await loadLocalFiles(station, dl.file);
            } else {
                await loadLocalFiles(station, selectedFile);
            }

            await renderFromLocal(station, product, status, loader, getSelectedFile());

        } catch (e) {
            alert("Error: " + e.message);
            status.innerText = "Fetch Failed";
        } finally {
            loader.classList.add('hidden');
        }
    }

    uiToggle.addEventListener('click', () => {
        const hidden = sidebar.classList.toggle('collapsed');
        uiToggle.classList.toggle('panel-hidden', hidden);
    });

    map.on('click', (evt) => {
        if (userMarker) map.removeLayer(userMarker);
        userMarker = L.marker(evt.latlng).addTo(map);
        userMarker.bindPopup(`Pinpoint<br>${evt.latlng.lat.toFixed(4)}, ${evt.latlng.lng.toFixed(4)}`).openPopup();
    });

    document.getElementById('pin-btn').addEventListener('click', () => {
        if (!navigator.geolocation) {
            alert('Geolocation is not supported by this browser.');
            return;
        }
        navigator.geolocation.getCurrentPosition(
            (pos) => {
                const loc = [pos.coords.latitude, pos.coords.longitude];
                if (userMarker) map.removeLayer(userMarker);
                userMarker = L.marker(loc).addTo(map);
                userMarker.bindPopup(`My Location<br>${loc[0].toFixed(4)}, ${loc[1].toFixed(4)}`).openPopup();
                map.setView(loc, 10);
            },
            () => alert('Could not retrieve your location.')
        );
    });

    document.getElementById('opacity-slider').addEventListener('input', (evt) => {
        const value = Number(evt.target.value);
        const label = document.getElementById('opacity-val');
        if (label) {
            label.textContent = `${Math.round(value * 100)}%`;
        }
        if (radarLayer) radarLayer.setOpacity(value);
    });

    document.getElementById('refresh-files-btn').addEventListener('click', async () => {
        const station = getStationInput();
        try {
            await loadLocalFiles(station, getSelectedFile());
        } catch (e) {
            alert('Error: ' + e.message);
        }
    });

    document.getElementById('station-id').addEventListener('change', async () => {
        const station = getStationInput();
        try {
            await loadLocalFiles(station);
        } catch (_) {
            // Ignore station-change refresh errors until user runs analysis.
        }
    });

    productSelect.addEventListener('change', () => {
        updateLegend(productSelect.value);
    });

    document.getElementById('pull-btn').addEventListener('click', () => fetchRadar(true));
    document.getElementById('load-btn').addEventListener('click', () => fetchRadar(false));

    loadLocalFiles(getStationInput()).catch(() => {
        // Keep defaults if local file list cannot be loaded on first paint.
    });

    updateLegend(productSelect.value);
});
