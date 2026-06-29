(function () {
    "use strict";

    const state = {
        basePath: localStorage.getItem("manga_base_path") || "",
        theme: localStorage.getItem("manga_theme") || "dark",
        currentManga: null,
        currentChapter: null,
        chapters: [],
        allImages: [],
        zoom: 1,
        recentlyRead: JSON.parse(localStorage.getItem("manga_recently_read") || "[]"),
        wishlist: JSON.parse(localStorage.getItem("manga_wishlist") || "[]"),
    };

    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);

    async function api(url, opts) {
        try {
            const r = await fetch(url, opts);
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return await r.json();
        } catch (e) {
            console.error("API error:", url, e);
            return null;
        }
    }

    function toast(msg, type) {
        const box = $("#toast-box");
        const el = document.createElement("div");
        el.className = "toast " + (type || "info");
        el.textContent = msg;
        box.appendChild(el);
        setTimeout(() => el.remove(), 3000);
    }

    function showPage(name) {
        $$(".page").forEach((p) => p.classList.remove("active"));
        const page = $("#page-" + name);
        if (page) page.classList.add("active");
    }

    function setTheme(t) {
        state.theme = t;
        document.documentElement.setAttribute("data-theme", t);
        localStorage.setItem("manga_theme", t);
    }

    function esc(s) {
        const d = document.createElement("div");
        d.textContent = s;
        return d.innerHTML;
    }

    // ── Recently Read ──────────────────────────────────────
    function saveRecentlyRead(manga, chapter) {
        const entry = {
            manga,
            chapter,
            time: Date.now(),
        };
        // Remove existing entry for same manga
        state.recentlyRead = state.recentlyRead.filter((r) => r.manga !== manga);
        // Add to front
        state.recentlyRead.unshift(entry);
        // Keep only last 10
        state.recentlyRead = state.recentlyRead.slice(0, 10);
        localStorage.setItem("manga_recently_read", JSON.stringify(state.recentlyRead));
    }

    function loadRecentlyRead() {
        const container = $("#recently-read");
        const list = $("#recent-list");
        if (!container || !list) return;

        if (state.recentlyRead.length === 0) {
            container.style.display = "none";
            return;
        }

        container.style.display = "block";
        list.innerHTML = "";

        state.recentlyRead.forEach((entry) => {
            const card = document.createElement("div");
            card.className = "recent-card";
            const timeAgo = getTimeAgo(entry.time);
            card.innerHTML =
                '<div class="recent-card-title">' + esc(entry.manga.replace(/-/g, " ")) + "</div>" +
                '<div class="recent-card-chapter">Chapter ' + entry.chapter + "</div>" +
                '<div class="recent-card-time">' + timeAgo + "</div>";
            card.addEventListener("click", () => navigateTo("reader", entry.manga, entry.chapter));
            list.appendChild(card);
        });
    }

    function getTimeAgo(timestamp) {
        const seconds = Math.floor((Date.now() - timestamp) / 1000);
        if (seconds < 60) return "Just now";
        if (seconds < 3600) return Math.floor(seconds / 60) + "m ago";
        if (seconds < 86400) return Math.floor(seconds / 3600) + "h ago";
        return Math.floor(seconds / 86400) + "d ago";
    }

    // ── Delete Manga/Chapter ───────────────────────────────
    async function deleteManga(slug) {
        if (!confirm("Delete all chapters for " + slug.replace(/-/g, " ") + "?")) return;
        const base = state.basePath || "";
        const data = await api("/api/manga/" + encodeURIComponent(slug) + "/delete" + (base ? "?path=" + encodeURIComponent(base) : ""), { method: "DELETE" });
        if (data && data.ok) {
            toast("Deleted", "success");
            loadMangaList();
        } else {
            toast("Delete failed", "error");
        }
    }

    async function deleteChapter(mangaSlug, chapterNum) {
        if (!confirm("Delete chapter " + chapterNum + "?")) return;
        const base = state.basePath || "";
        const data = await api("/api/manga/" + encodeURIComponent(mangaSlug) + "/chapter/" + chapterNum + "/delete" + (base ? "?path=" + encodeURIComponent(base) : ""), { method: "DELETE" });
        if (data && data.ok) {
            toast("Chapter deleted", "success");
            openManga(mangaSlug);
        } else {
            toast("Delete failed", "error");
        }
    }

    // ── Wishlist ───────────────────────────────────────────
    function addToWishlist(title, url) {
        const entry = { title, url, added: Date.now() };
        if (!state.wishlist.find((w) => w.url === url)) {
            state.wishlist.push(entry);
            localStorage.setItem("manga_wishlist", JSON.stringify(state.wishlist));
            toast("Added to wishlist", "success");
        }
    }

    function removeFromWishlist(url) {
        state.wishlist = state.wishlist.filter((w) => w.url !== url);
        localStorage.setItem("manga_wishlist", JSON.stringify(state.wishlist));
        loadWishlist();
    }

    function loadWishlist() {
        const list = $("#wishlist-list");
        if (!list) return;

        list.innerHTML = "";
        state.wishlist.forEach((entry) => {
            const card = document.createElement("div");
            card.className = "wishlist-card";
            card.innerHTML =
                '<div class="wishlist-card-info">' +
                '<div class="wishlist-card-title">' + esc(entry.title) + "</div>" +
                '<div class="wishlist-card-url">' + esc(entry.url) + "</div>" +
                "</div>" +
                '<div class="wishlist-card-actions">' +
                '<button class="btn btn-primary btn-sm scrape-btn" data-url="' + esc(entry.url) + '">Scrape</button>' +
                '<button class="delete-btn remove-btn" data-url="' + esc(entry.url) + '">Remove</button>' +
                "</div>";
            list.appendChild(card);
        });

        // Add event listeners
        list.querySelectorAll(".scrape-btn").forEach((btn) => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                window.location.href = "/scraper.html?url=" + encodeURIComponent(btn.dataset.url);
            });
        });

        list.querySelectorAll(".remove-btn").forEach((btn) => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                removeFromWishlist(btn.dataset.url);
            });
        });
    }

    // ── Hash routing (survives refresh) ─────────────────────
    function navigateTo(page, a, b) {
        if (page === "detail") window.location.hash = "#/detail/" + encodeURIComponent(a);
        else if (page === "reader") window.location.hash = "#/reader/" + encodeURIComponent(a) + "/" + b;
        else window.location.hash = "#/";
    }

    function handleHash() {
        const hash = window.location.hash || "#/";
        const parts = decodeURIComponent(hash).slice(2).split("/"); // remove "#/"

        if (parts[0] === "reader" && parts[1] && parts[2] != null) {
            state.currentManga = parts[1];
            // reload chapters list then open reader
            loadChaptersFor(parts[1]).then(() => openReader(parseFloat(parts[2])));
        } else if (parts[0] === "detail" && parts[1]) {
            openManga(parts[1]);
        } else {
            showPage("home");
            loadMangaList();
            loadRecentlyRead();
        }
    }

    // ── Theme ──────────────────────────────────────────────
    $("#theme-toggle").addEventListener("click", () => {
        setTheme(state.theme === "dark" ? "light" : "dark");
    });

    // ── Home Page ──────────────────────────────────────────
    async function loadMangaList() {
        const grid = $("#manga-grid");
        const empty = $("#empty-state");
        grid.innerHTML = '<div style="text-align:center;padding:2rem;color:var(--fg2)">Loading...</div>';
        empty.style.display = "none";

        const base = state.basePath || "";
        const data = await api("/api/manga" + (base ? "?path=" + encodeURIComponent(base) : ""));
        grid.innerHTML = "";

        if (!data || !data.manga || data.manga.length === 0) {
            empty.style.display = "block";
            return;
        }

        data.manga.forEach((m) => {
            const card = document.createElement("div");
            card.className = "manga-card";
            card.innerHTML =
                '<div class="manga-card-cover"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M20 17V7a2 2 0 0 0-2-2H6.5A2.5 2.5 0 0 0 4 7.5v12"/></svg></div>' +
                '<div class="manga-card-body">' +
                '<div class="manga-card-title">' + esc(m.title) + "</div>" +
                '<div class="manga-card-meta">' + m.chapters + " chapters</div>" +
                "</div>";
            card.addEventListener("click", () => navigateTo("detail", m.slug));
            grid.appendChild(card);
        });
    }

    $("#scan-btn").addEventListener("click", () => {
        state.basePath = $("#path-input").value.trim();
        localStorage.setItem("manga_base_path", state.basePath);
        loadMangaList();
    });

    $("#path-input").addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            state.basePath = e.target.value.trim();
            localStorage.setItem("manga_base_path", state.basePath);
            loadMangaList();
        }
    });

    // ── Sites Grid ─────────────────────────────────────────
    // ── Manga Detail ───────────────────────────────────────
    async function loadChaptersFor(slug) {
        const base = state.basePath || "";
        const data = await api("/api/manga/" + encodeURIComponent(slug) + (base ? "?path=" + encodeURIComponent(base) : ""));
        if (data) state.chapters = data.chapters || [];
    }

    async function openManga(slug) {
        state.currentManga = slug;
        const base = state.basePath || "";
        const data = await api("/api/manga/" + encodeURIComponent(slug) + (base ? "?path=" + encodeURIComponent(base) : ""));
        if (!data) {
            toast("Failed to load manga", "error");
            return;
        }

        state.chapters = data.chapters || [];
        $("#detail-title").textContent = data.title;
        $("#detail-chapters").textContent = state.chapters.length + " chapters";

        const contBtn = $("#continue-btn");
        if (data.progress && data.progress.last_chapter != null) {
            contBtn.style.display = "inline-flex";
            contBtn.onclick = () => navigateTo("reader", slug, data.progress.last_chapter);
        } else {
            contBtn.style.display = "none";
        }

        // Add delete manga button
        let deleteBtn = $("#delete-manga-btn");
        if (!deleteBtn) {
            deleteBtn = document.createElement("button");
            deleteBtn.id = "delete-manga-btn";
            deleteBtn.className = "delete-btn";
            deleteBtn.textContent = "Delete Manga";
            $("#detail-chapters").parentNode.insertBefore(deleteBtn, $("#detail-chapters").nextSibling);
        }
        deleteBtn.onclick = () => deleteManga(slug);

        renderChapters(state.chapters);
        renderComments(data.comments || []);
        showPage("detail");
    }

    function renderChapters(chapters) {
        const list = $("#chapter-list");
        list.innerHTML = "";
        chapters.forEach((ch) => {
            const el = document.createElement("div");
            el.className = "chapter-item";
            el.innerHTML =
                '<div class="chapter-num">' + ch.number + "</div>" +
                '<div class="chapter-info">' +
                '<div class="chapter-name">Chapter ' + ch.number + "</div>" +
                '<div class="chapter-meta">' + ch.file + "</div>" +
                "</div>" +
                '<button class="delete-btn chapter-delete-btn" data-num="' + ch.number + '">Delete</button>';
            el.addEventListener("click", (e) => {
                if (!e.target.classList.contains("chapter-delete-btn")) {
                    navigateTo("reader", state.currentManga, ch.number);
                }
            });
            list.appendChild(el);
        });

        // Add delete event listeners
        list.querySelectorAll(".chapter-delete-btn").forEach((btn) => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                deleteChapter(state.currentManga, parseFloat(btn.dataset.num));
            });
        });
    }

    $("#back-btn").addEventListener("click", () => {
        state.currentManga = null;
        navigateTo("home");
    });

    $("#chapter-search").addEventListener("input", (e) => {
        const q = e.target.value.toLowerCase();
        const filtered = state.chapters.filter(
            (ch) => ch.number.toString().includes(q) || ch.file.toLowerCase().includes(q)
        );
        renderChapters(filtered);
    });

    // ── Comments ───────────────────────────────────────────
    function renderComments(list) {
        const el = $("#comments-list");
        if (!list.length) {
            el.innerHTML = '<div class="no-comments">No comments yet.</div>';
            return;
        }
        el.innerHTML = "";
        list.forEach((c) => {
            const card = document.createElement("div");
            card.className = "comment-card";
            card.innerHTML =
                '<div class="comment-header"><span class="comment-author">' +
                esc(c.name || "Anonymous") +
                '</span><span class="comment-date">' +
                new Date(c.date).toLocaleDateString() +
                "</span></div>" +
                '<div class="comment-body">' + esc(c.text) + "</div>";
            el.appendChild(card);
        });
    }

    $("#comment-submit").addEventListener("click", async () => {
        const name = $("#comment-name").value.trim() || "Anonymous";
        const text = $("#comment-text").value.trim();
        if (!text) return toast("Write something first", "error");
        if (!state.currentManga) return;

        const data = await api("/api/manga/" + encodeURIComponent(state.currentManga) + "/comments", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name, text, date: new Date().toISOString() }),
        });

        if (data) {
            $("#comment-text").value = "";
            toast("Comment posted", "success");
            const manga = await api("/api/manga/" + encodeURIComponent(state.currentManga) + (state.basePath ? "?path=" + encodeURIComponent(state.basePath) : ""));
            if (manga) renderComments(manga.comments || []);
        }
    });

    // ── Reader (vertical scroll) ───────────────────────────
    function getImageUrl(chapterNum, imageName) {
        const base = state.basePath || "";
        let url =
            "/api/manga/" +
            encodeURIComponent(state.currentManga) +
            "/chapter/" +
            chapterNum +
            "/image/" +
            encodeURIComponent(imageName);
        if (base) url += "?path=" + encodeURIComponent(base);
        return url;
    }

    async function openReader(chapterNum) {
        state.currentChapter = chapterNum;
        const base = state.basePath || "";
        const data = await api(
            "/api/manga/" +
                encodeURIComponent(state.currentManga) +
                "/chapter/" +
                chapterNum +
                (base ? "?path=" + encodeURIComponent(base) : "")
        );

        if (!data || !data.images || !data.images.length) {
            toast("No images found in this chapter", "error");
            return;
        }

        state.allImages = data.images;

        // Save progress
        api("/api/manga/" + encodeURIComponent(state.currentManga) + "/progress", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ last_chapter: chapterNum }),
        });

        // Save to recently read
        saveRecentlyRead(state.currentManga, chapterNum);

        $("#reader-chapter-label").textContent = "Chapter " + chapterNum;
        state.zoom = 1;
        applyZoom();

        // Build all image elements
        const container = $("#reader-pages");
        container.innerHTML = "";

        data.images.forEach((imgName, i) => {
            const img = document.createElement("img");
            img.loading = "lazy";
            img.alt = "Page " + (i + 1);
            img.src = getImageUrl(chapterNum, imgName);
            container.appendChild(img);
        });

        showPage("reader");

        // Scroll to top
        const body = $("#reader-body");
        body.scrollTop = 0;
    }

    // Zoom controls
    $("#zoom-in").addEventListener("click", () => {
        state.zoom = Math.min(3, state.zoom + 0.25);
        applyZoom();
    });

    $("#zoom-out").addEventListener("click", () => {
        state.zoom = Math.max(0.5, state.zoom - 0.25);
        applyZoom();
    });

    function applyZoom() {
        const container = $("#reader-pages");
        // Change width directly — images are width:100% so they resize naturally
        container.style.width = Math.round(900 * state.zoom) + "px";
        $("#zoom-label").textContent = Math.round(state.zoom * 100) + "%";
    }

    // Prev/Next chapter — use == null instead of ! to allow chapter 0
    $("#prev-chapter").addEventListener("click", () => {
        if (!state.chapters.length || state.currentChapter == null) return;
        const idx = state.chapters.findIndex((c) => c.number === state.currentChapter);
        if (idx > 0) navigateTo("reader", state.currentManga, state.chapters[idx - 1].number);
        else toast("First chapter", "info");
    });

    $("#next-chapter").addEventListener("click", () => {
        if (!state.chapters.length || state.currentChapter == null) return;
        const idx = state.chapters.findIndex((c) => c.number === state.currentChapter);
        if (idx >= 0 && idx < state.chapters.length - 1) navigateTo("reader", state.currentManga, state.chapters[idx + 1].number);
        else toast("Last chapter", "info");
    });

    $("#reader-back").addEventListener("click", () => {
        if (state.currentManga) navigateTo("detail", state.currentManga);
        else navigateTo("home");
    });

    // Scroll progress tracking
    $("#reader-body").addEventListener("scroll", function () {
        const el = this;
        const pct = Math.round((el.scrollTop / (el.scrollHeight - el.clientHeight)) * 100) || 0;
        $("#scroll-progress").textContent = Math.min(pct, 100) + "%";
    });

    // Keyboard shortcuts
    document.addEventListener("keydown", (e) => {
        if (!$("#page-reader").classList.contains("active")) return;
        if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;

        const body = $("#reader-body");
        const step = 300;

        if (e.key === "ArrowDown" || e.key === " ") {
            e.preventDefault();
            body.scrollBy({ top: step, behavior: "smooth" });
        } else if (e.key === "ArrowUp") {
            e.preventDefault();
            body.scrollBy({ top: -step, behavior: "smooth" });
        } else if (e.key === "Escape") {
            if (state.currentManga) navigateTo("detail", state.currentManga);
            else navigateTo("home");
        }
    });

    // Mouse wheel zoom with Ctrl
    $("#reader-body").addEventListener("wheel", (e) => {
        if (e.ctrlKey || e.metaKey) {
            e.preventDefault();
            if (e.deltaY < 0) {
                state.zoom = Math.min(3, state.zoom + 0.1);
            } else {
                state.zoom = Math.max(0.5, state.zoom - 0.1);
            }
            applyZoom();
        }
    }, { passive: false });

    // ── Init ───────────────────────────────────────────────
    setTheme(state.theme);
    $("#path-input").value = state.basePath;

    // Listen for hash changes (back/forward/refresh)
    window.addEventListener("hashchange", handleHash);
    handleHash();
})();
