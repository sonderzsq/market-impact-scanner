const API = {
    articles: '/api/articles',
    stats: '/api/stats',
    sources: '/api/sources',
    fetch: '/api/fetch',
    analyze: '/api/analyze',
    marketSummary: '/api/market-summary',
};

const SECTOR_MAP = {
    tmt: {
        label: 'TMT',
        match: ['technology', 'communications', 'media', 'telecom'],
    },
    defensive: {
        label: 'Defensive',
        match: ['healthcare', 'utilities', 'consumer staples', 'consumer'],
    },
    macro: {
        label: 'Macroeconomics',
        match: ['broad market', 'bonds', 'commodities', 'crypto'],
    },
    cyclical: {
        label: 'Cyclical',
        match: ['finance', 'energy', 'industrial', 'real estate', 'materials'],
    },
};

const state = {
    articles: [],
    articlesById: {},
    activeTab: 'overview',
    refreshTimer: null,
};

const $grid = document.getElementById('articles-grid');
const $status = document.getElementById('status-bar');
const $filterImpact = document.getElementById('filter-impact');
const $filterSource = document.getElementById('filter-source');
const $sortBy = document.getElementById('sort-by');
const $sortOrder = document.getElementById('sort-order');
const $btnFetch = document.getElementById('btn-fetch');
const $btnAnalyze = document.getElementById('btn-analyze');
const $sectorStatus = document.getElementById('sector-status-bar');

document.addEventListener('DOMContentLoaded', () => {
    loadArticles();
    loadStats();
    loadSources();
    loadMarketSummary();

    $filterImpact.addEventListener('change', loadArticles);
    $filterSource.addEventListener('change', loadArticles);
    $sortBy.addEventListener('change', loadArticles);
    $sortOrder.addEventListener('change', loadArticles);

    $btnFetch.addEventListener('click', fetchFeeds);
    $btnAnalyze.addEventListener('click', analyzeArticles);

    document.getElementById('btn-fetch-sectors').addEventListener('click', fetchFeeds);
    document.getElementById('btn-analyze-sectors').addEventListener('click', analyzeArticles);

    document.querySelectorAll('.sector-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.sector-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            openSectorDetail(btn.dataset.sector);
        });
    });

    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => switchTab(tab.dataset.tab));
    });

    document.getElementById('modal-overlay').addEventListener('click', (e) => {
        if (e.target.id === 'modal-overlay') closeModal();
    });
    document.getElementById('modal-close').addEventListener('click', closeModal);
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') closeModal();
    });

    document.body.addEventListener('click', (e) => {
        const card = e.target.closest('.article-card, .sector-card');
        if (!card) return;
        if (e.target.closest('a')) return;
        const articleId = card.dataset.articleId;
        if (articleId && state.articlesById[articleId]) {
            openModal(state.articlesById[articleId]);
        }
    });

    state.refreshTimer = setInterval(() => {
        loadArticles();
        loadStats();
        loadMarketSummary();
    }, 30000);
});

function switchTab(tabName) {
    state.activeTab = tabName;

    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

    document.querySelector(`.tab[data-tab="${tabName}"]`).classList.add('active');
    document.getElementById(`tab-${tabName}`).classList.add('active');

    if (tabName === 'sectors') {
        const activeBtn = document.querySelector('.sector-btn.active');
        const activeSector = activeBtn ? activeBtn.dataset.sector : 'tmt';
        openSectorDetail(activeSector);
    }
}

async function loadArticles() {
    const params = new URLSearchParams({
        impact_level: $filterImpact.value,
        source: $filterSource.value,
        sort_by: $sortBy.value,
        sort_order: $sortOrder.value,
        limit: '500',
    });

    try {
        const res = await fetch(`${API.articles}?${params}`);
        const articles = await res.json();
        state.articles = articles;
        indexArticles(articles);
        renderArticles(articles);
        updateRefreshTime();
        if (state.activeTab === 'sectors') {
            const activeBtn = document.querySelector('.sector-btn.active');
            if (activeBtn) openSectorDetail(activeBtn.dataset.sector);
        }
    } catch (err) {
        showStatus('Failed to load articles: ' + err.message, 'error');
    }
}

async function loadAllAnalyzedArticles() {
    const params = new URLSearchParams({
        impact_level: 'all',
        source: 'all',
        sort_by: 'impact_score',
        sort_order: 'DESC',
        limit: '500',
    });
    try {
        const res = await fetch(`${API.articles}?${params}`);
        return await res.json();
    } catch {
        return state.articles;
    }
}

async function loadStats() {
    try {
        const res = await fetch(API.stats);
        const stats = await res.json();
        document.getElementById('stat-total').textContent = stats.total || 0;
        document.getElementById('stat-high').textContent = stats.high_impact || 0;
        document.getElementById('stat-medium').textContent = stats.medium_impact || 0;
        document.getElementById('stat-low').textContent = stats.low_impact || 0;
    } catch (err) {
        console.error('Failed to load stats:', err);
    }
}

async function loadSources() {
    try {
        const res = await fetch(API.sources);
        const sources = await res.json();
        const current = $filterSource.value;
        $filterSource.innerHTML = '<option value="all">All Sources</option>';
        sources.forEach(s => {
            const opt = document.createElement('option');
            opt.value = s;
            opt.textContent = s;
            if (s === current) opt.selected = true;
            $filterSource.appendChild(opt);
        });
    } catch (err) {
        console.error('Failed to load sources:', err);
    }
}

const DIRECTION_SYMBOLS = { bullish: '\u25B2', bearish: '\u25BC', mixed: '\u25C6', neutral: '\u2014' };

async function loadMarketSummary() {
    try {
        const res = await fetch(API.marketSummary);
        const data = await res.json();
        renderMarketSummary(data);
    } catch (err) {
        console.error('Failed to load market summary:', err);
    }
}

function renderMarketSummary(data) {
    if (!data || data.total_analyzed === 0) {
        document.getElementById('ms-direction-arrow').textContent = '\u2014';
        document.getElementById('ms-direction-arrow').className = 'ms-direction-arrow neutral';
        document.getElementById('ms-direction-label').textContent = 'NO DATA';
        document.getElementById('ms-direction-label').className = 'ms-direction-label neutral';
        document.getElementById('ms-total-analyzed').textContent = '0';
        document.getElementById('ms-avg-score').textContent = '0';
        document.getElementById('ms-bullish-count').textContent = '0';
        document.getElementById('ms-bearish-count').textContent = '0';
        document.getElementById('ms-neutral-count').textContent = '0';
        document.getElementById('ms-mixed-count').textContent = '0';
        document.getElementById('ms-drivers-list').innerHTML = '<div class="ms-loading">No analyzed articles yet. Click Analyze to get started.</div>';
        document.getElementById('ms-sectors-grid').innerHTML = '';
        return;
    }

    const dir = data.overall_direction || 'neutral';
    const arrow = DIRECTION_SYMBOLS[dir] || '\u2014';

    document.getElementById('ms-direction-arrow').textContent = arrow;
    document.getElementById('ms-direction-arrow').className = `ms-direction-arrow ${dir}`;
    document.getElementById('ms-direction-label').textContent = dir.toUpperCase();
    document.getElementById('ms-direction-label').className = `ms-direction-label ${dir}`;

    document.getElementById('ms-total-analyzed').textContent = data.total_analyzed;
    document.getElementById('ms-avg-score').textContent = data.avg_score;

    const bd = data.direction_breakdown || {};
    document.getElementById('ms-bullish-count').textContent = bd.bullish || 0;
    document.getElementById('ms-bearish-count').textContent = bd.bearish || 0;
    document.getElementById('ms-neutral-count').textContent = bd.neutral || 0;
    document.getElementById('ms-mixed-count').textContent = bd.mixed || 0;

    const driversList = document.getElementById('ms-drivers-list');
    if (data.top_drivers && data.top_drivers.length > 0) {
        driversList.innerHTML = data.top_drivers.map(d => {
            const dDir = d.market_direction || 'neutral';
            const dArrow = DIRECTION_SYMBOLS[dDir] || '\u2014';
            const scoreColor = d.impact_level === 'high' ? 'var(--impact-high)' :
                               d.impact_level === 'medium' ? 'var(--impact-medium)' : 'var(--impact-low)';
            const summary = d.impact_summary ? `<div class="ms-driver-summary">${escapeHtml(d.impact_summary)}</div>` : '';
            return `
            <div class="ms-driver" data-article-url="${escapeHtml(d.url || '')}">
                <span class="ms-driver-arrow direction-indicator ${dDir}">${dArrow}</span>
                <div class="ms-driver-content">
                    <div class="ms-driver-title">${escapeHtml(d.title)}</div>
                    <div class="ms-driver-meta">
                        <span class="ms-driver-source">${escapeHtml(d.source)}</span>
                    </div>
                    ${summary}
                </div>
                <span class="ms-driver-score" style="color:${scoreColor}">${d.impact_score}</span>
            </div>`;
        }).join('');
    } else {
        driversList.innerHTML = '<div class="ms-loading">No key drivers identified yet.</div>';
    }

    const sectorsGrid = document.getElementById('ms-sectors-grid');
    const sectorEntries = Object.entries(data.sector_sentiment || {});
    if (sectorEntries.length > 0) {
        sectorsGrid.innerHTML = sectorEntries.map(([name, info]) => {
            const sDir = info.direction || 'neutral';
            const sArrow = DIRECTION_SYMBOLS[sDir] || '\u2014';
            return `
            <div class="ms-sector-pill ${sDir}">
                <span class="ms-pill-arrow">${sArrow}</span>
                <span class="ms-pill-name">${escapeHtml(name)}</span>
                <span class="ms-pill-count">${info.count}</span>
            </div>`;
        }).join('');
    } else {
        sectorsGrid.innerHTML = '';
    }
}

async function fetchFeeds() {
    setBothButtons('fetch', true, '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;vertical-align:middle;margin-right:6px;"></span> Fetching...');
    showStatus('Fetching RSS feeds...', 'info');

    try {
        const res = await fetch(API.fetch, { method: 'POST' });
        const data = await res.json();
        showStatus(
            `Fetched ${data.total_fetched || 0} articles. ${data.new_articles || 0} new, ${data.duplicates || 0} duplicates.`,
            'success'
        );
        loadArticles();
        loadStats();
        loadSources();
        loadMarketSummary();
    } catch (err) {
        showStatus('Feed fetch failed: ' + err.message, 'error');
    } finally {
        setBothButtons('fetch', false, '<span class="btn-icon">&#8635;</span> Refresh Feeds');
    }
}

async function analyzeArticles() {
    setBothButtons('analyze', true, '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;vertical-align:middle;margin-right:6px;"></span> Analyzing...');
    showStatus('Analyzing articles with LLM... This may take a minute.', 'info');

    try {
        const res = await fetch(`${API.analyze}?batch_size=20`, { method: 'POST' });
        const data = await res.json();
        if (data.error) {
            showStatus(data.error + (data.fix ? ' ' + data.fix : ''), 'error');
        } else {
            showStatus(
                `Analyzed ${data.analyzed || 0} of ${data.total || 0} articles. ${data.failed || 0} failed.`,
                'success'
            );
        }
        loadArticles();
        loadStats();
        loadMarketSummary();
    } catch (err) {
        showStatus('Analysis failed: ' + err.message, 'error');
    } finally {
        setBothButtons('analyze', false, '<span class="btn-icon">&#9881;</span> Analyze');
    }
}

function setBothButtons(type, disabled, html) {
    const ids = type === 'fetch'
        ? ['btn-fetch', 'btn-fetch-sectors']
        : ['btn-analyze', 'btn-analyze-sectors'];
    ids.forEach(id => {
        const el = document.getElementById(id);
        el.disabled = disabled;
        el.innerHTML = html;
    });
}

function renderArticles(articles) {
    if (!articles || articles.length === 0) {
        $grid.innerHTML = `
            <div class="empty-state">
                <p>No articles yet. Click <strong>Refresh Feeds</strong> to fetch news.</p>
            </div>`;
        return;
    }

    $grid.innerHTML = articles.map(articleCardHtml).join('');
}

function articleCardHtml(article) {
    const impact = article.impact_level || 'unanalyzed';
    const score = article.impact_score || 0;
    const direction = article.market_direction || 'neutral';
    const sectors = parseSectors(article.affected_sectors);
    const dirSymbol = getDirectionSymbol(direction);
    const timeAgo = relativeTime(article.published_at);

    return `
    <div class="article-card impact-${impact}" data-article-id="${article.id}">
        <div class="card-header">
            <div class="card-title">
                ${escapeHtml(article.title)}
            </div>
            <div class="card-meta">
                <span class="impact-badge ${impact}">${impact}</span>
                ${score > 0 ? `<span class="impact-score">${score}</span>` : ''}
                <span class="direction-indicator ${direction}" title="${direction}">${dirSymbol}</span>
            </div>
        </div>
        ${score > 0 ? `
        <div class="score-bar-container">
            <div class="score-bar ${impact}" style="width: ${score}%"></div>
        </div>` : ''}
        <div class="card-source-row">
            <span class="card-source">${escapeHtml(article.source)}</span>
            <span class="card-date">${timeAgo}</span>
        </div>
        ${article.impact_summary ? `<div class="card-summary">${escapeHtml(article.impact_summary)}</div>` : ''}
        ${sectors.length > 0 ? `
        <div class="card-sectors">
            ${sectors.map(s => `<span class="sector-chip">${escapeHtml(s)}</span>`).join('')}
        </div>` : ''}
    </div>`;
}

function classifyArticleToSectors(article) {
    const sectors = parseSectors(article.affected_sectors);
    if (!sectors.length) return [];

    const matched = [];
    for (const [key, config] of Object.entries(SECTOR_MAP)) {
        const hit = sectors.some(s =>
            config.match.some(m => s.toLowerCase().includes(m))
        );
        if (hit) matched.push(key);
    }
    return matched;
}

function buildSectorBuckets(articles) {
    const buckets = { tmt: [], defensive: [], macro: [], cyclical: [] };
    for (const article of articles) {
        const categories = classifyArticleToSectors(article);
        for (const cat of categories) {
            buckets[cat].push(article);
        }
    }
    return buckets;
}

async function openSectorDetail(sectorKey) {
    const config = SECTOR_MAP[sectorKey];
    if (!config) return;

    const allArticles = await loadAllAnalyzedArticles();
    indexArticles(allArticles);
    const analyzed = allArticles.filter(a => a.impact_level && a.impact_level !== 'unanalyzed');

    const buckets = buildSectorBuckets(analyzed);
    for (const key of Object.keys(buckets)) {
        const el = document.getElementById(`count-${key}`);
        if (el) el.textContent = buckets[key].length;
    }

    const sectorArticles = buckets[sectorKey] || [];
    sectorArticles.sort((a, b) => (b.impact_score || 0) - (a.impact_score || 0));

    const high = sectorArticles.filter(a => a.impact_level === 'high');
    const medium = sectorArticles.filter(a => a.impact_level === 'medium');
    const low = sectorArticles.filter(a => a.impact_level === 'low' || a.impact_level === 'none');

    renderImpactColumn('high', high);
    renderImpactColumn('medium', medium);
    renderImpactColumn('low', low);
}

function renderImpactColumn(level, articles) {
    document.getElementById(`col-count-${level}`).textContent = articles.length;
    const body = document.getElementById(`col-${level}`);

    if (articles.length === 0) {
        body.innerHTML = '<div class="sector-empty">No articles at this impact level.</div>';
        return;
    }

    body.innerHTML = articles.map(article => {
        const score = article.impact_score || 0;
        const direction = article.market_direction || 'neutral';
        const dirSymbol = getDirectionSymbol(direction);
        const timeAgo = relativeTime(article.published_at);

        return `
        <div class="sector-card" data-article-id="${article.id}">
            <div class="sector-card-row">
                <span class="sector-card-score" style="color: var(--impact-${article.impact_level || 'none'})">${score}</span>
                <span class="direction-indicator ${direction}" style="font-size:14px;">${dirSymbol}</span>
            </div>
            <div class="sector-card-title">${escapeHtml(article.title)}</div>
            <div class="sector-card-row">
                <span class="sector-card-source">${escapeHtml(article.source)}</span>
                <span class="sector-card-source">${timeAgo}</span>
            </div>
            ${article.impact_summary ? `<div class="sector-card-summary">${escapeHtml(article.impact_summary)}</div>` : ''}
        </div>`;
    }).join('');
}

function indexArticles(articles) {
    for (const a of articles) {
        if (a.id) state.articlesById[a.id] = a;
    }
}

function openModal(article) {
    const overlay = document.getElementById('modal-overlay');
    const impact = article.impact_level || 'unanalyzed';
    const score = article.impact_score || 0;
    const direction = article.market_direction || 'neutral';
    const sectors = parseSectors(article.affected_sectors);
    const dirSymbol = getDirectionSymbol(direction);
    const dirLabel = direction.charAt(0).toUpperCase() + direction.slice(1);

    const badge = document.getElementById('modal-impact-badge');
    badge.textContent = impact;
    badge.className = `impact-badge ${impact}`;

    const scoreEl = document.getElementById('modal-impact-score');
    scoreEl.textContent = score > 0 ? score : '';

    const dirEl = document.getElementById('modal-direction');
    dirEl.textContent = dirSymbol;
    dirEl.className = `direction-indicator ${direction}`;

    document.getElementById('modal-title').textContent = article.title || '';
    document.getElementById('modal-source').textContent = article.source || '';
    document.getElementById('modal-date').textContent = article.published_at
        ? relativeTime(article.published_at) + ' \u2022 ' + new Date(article.published_at).toLocaleString()
        : '';

    const bar = document.getElementById('modal-score-bar');
    bar.style.width = score + '%';
    bar.className = `score-bar ${impact}`;

    const summaryText = article.summary || '';
    const summaryEl = document.getElementById('modal-summary');
    summaryEl.textContent = summaryText || 'No summary available for this article.';

    const analysisSection = document.getElementById('modal-analysis-section');
    const analysisEl = document.getElementById('modal-analysis');
    if (article.impact_summary) {
        analysisEl.textContent = article.impact_summary;
        analysisSection.style.display = '';
    } else {
        analysisSection.style.display = 'none';
    }

    const dirSection = document.getElementById('modal-direction-section');
    const dirDetail = document.getElementById('modal-direction-detail');
    if (impact !== 'unanalyzed') {
        dirDetail.innerHTML = `<span class="direction-indicator ${direction}" style="font-size:20px;">${dirSymbol}</span>` +
            `<span class="modal-direction-label ${direction}">${escapeHtml(dirLabel)}</span>`;
        dirSection.style.display = '';
    } else {
        dirSection.style.display = 'none';
    }

    const sectorsSection = document.getElementById('modal-sectors-section');
    const sectorsEl = document.getElementById('modal-sectors');
    if (sectors.length > 0) {
        sectorsEl.innerHTML = sectors.map(s =>
            `<span class="sector-chip">${escapeHtml(s)}</span>`
        ).join('');
        sectorsSection.style.display = '';
    } else {
        sectorsSection.style.display = 'none';
    }

    document.getElementById('modal-link').href = article.url || '#';

    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';
}

function closeModal() {
    document.getElementById('modal-overlay').classList.remove('open');
    document.body.style.overflow = '';
}

function parseSectors(raw) {
    if (!raw) return [];
    try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
    } catch {
        return raw.split(',').map(s => s.trim()).filter(Boolean);
    }
}

function getDirectionSymbol(direction) {
    switch (direction) {
        case 'bullish': return '\u25B2';
        case 'bearish': return '\u25BC';
        case 'mixed': return '\u25C6';
        default: return '\u2014';
    }
}

function relativeTime(isoStr) {
    if (!isoStr) return '';
    const now = Date.now();
    const then = new Date(isoStr).getTime();
    const diff = now - then;
    const minutes = Math.floor(diff / 60000);
    const hours = Math.floor(diff / 3600000);
    const days = Math.floor(diff / 86400000);

    if (minutes < 1) return 'just now';
    if (minutes < 60) return `${minutes}m ago`;
    if (hours < 24) return `${hours}h ago`;
    if (days < 7) return `${days}d ago`;
    return new Date(isoStr).toLocaleDateString();
}

function updateRefreshTime() {
    const now = new Date();
    document.getElementById('last-refresh').textContent =
        'Updated ' + now.toLocaleTimeString();
}

function showStatus(msg, type) {
    $status.textContent = msg;
    $status.className = `status-bar ${type}`;
    $sectorStatus.textContent = msg;
    $sectorStatus.className = `status-bar ${type}`;
    if (type === 'success') {
        setTimeout(() => {
            $status.className = 'status-bar hidden';
            $sectorStatus.className = 'status-bar hidden';
        }, 5000);
    }
}

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
