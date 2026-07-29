# main.py - Исправленная версия
import asyncio
import zipfile
import io
from datetime import datetime
from typing import List, Dict, Optional
import aiohttp
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from collections import defaultdict
import pytz
import logging
import random

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="File Analyzer Service")

# Разрешаем CORS для локальной разработки
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============ КОНФИГУРАЦИЯ ============
API_BASE_URL = "http://91.199.149.128:18001"  # Реальный URL из задания
CANDIDATE_ID = "beluncho.github.io"  # Твой ID
DOWNLOAD_DELAY = (2, 6)

# ============ ХРАНИЛИЩЕ ============
class Storage:
    """In-memory хранилище для файлов и их статистики"""

    def __init__(self):
        self.files: Dict[str, str] = {}  # имя файла -> содержимое
        self.download_time: Dict[str, datetime] = {}  # имя файла -> время скачивания
        self.total_files = 0  # Общее количество файлов в каталоге (узнаем в процессе)
        self.downloaded_count = 0
        self.is_downloading = False
        self.start_time: Optional[datetime] = None

    def add_file(self, filename: str, content: str) -> bool:
        """Добавить файл в хранилище"""
        if filename not in self.files:
            self.files[filename] = content
            self.download_time[filename] = datetime.now(pytz.timezone('Asia/Novosibirsk'))
            self.downloaded_count += 1
            return True
        return False

    def get_file_stats(self, filename: str) -> Dict[int, int]:
        """Подсчитать статистику для одного файла (500 цифр)"""
        if filename not in self.files:
            return {}

        content = self.files[filename]
        stats = defaultdict(int)
        for char in content.strip():
            if char.isdigit():
                stats[int(char)] += 1
        return dict(stats)

    def get_combined_stats(self, filenames: List[str]) -> Dict[int, int]:
        """Подсчитать общую статистику для выбранных файлов"""
        combined = defaultdict(int)
        for filename in filenames:
            if filename in self.files:
                stats = self.get_file_stats(filename)
                for digit, count in stats.items():
                    combined[digit] += count
        return dict(combined)

    def get_files_with_time(self) -> List[Dict]:
        """Получить все файлы с временем скачивания (сортировка по убыванию)"""
        return [
            {
                "name": name,
                "time": dt.strftime("%Y-%m-%d %H:%M:%S")
            }
            for name, dt in sorted(
                self.download_time.items(),
                key=lambda x: x[1],
                reverse=True
            )
        ]

    def clear(self):
        """Очистить хранилище (для тестирования)"""
        self.files.clear()
        self.download_time.clear()
        self.total_files = 0
        self.downloaded_count = 0
        self.is_downloading = False
        self.start_time = None


storage = Storage()


# ============ КЛИЕНТ API ============
class APIClient:
    """Клиент для взаимодействия с API файлов"""

    def __init__(self, base_url: str, candidate_id: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.candidate_id = candidate_id
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def _get_headers(self) -> Dict:
        """Получить заголовки для запроса"""
        headers = {}
        if self.candidate_id:
            headers["X-Candidate-Id"] = self.candidate_id
        return headers

    async def _request_with_retry(self, method: str, url: str, **kwargs):
        """
        Выполнить запрос с обработкой ошибок и retry
        """
        max_retries = 5
        base_delay = 2

        for attempt in range(max_retries):
            try:
                # Добавляем заголовки
                if 'headers' not in kwargs:
                    kwargs['headers'] = {}
                kwargs['headers'].update(self._get_headers())

                async with self.session.request(method, url, **kwargs) as response:
                    # Обработка 429 - Too Many Requests
                    if response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 5))
                        logger.warning(f"Rate limit exceeded. Waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    # Обработка 403 - Blocked
                    if response.status == 403:
                        retry_after = int(response.headers.get('Retry-After', 1800))
                        logger.warning(f"Blocked for {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    # Успешный ответ
                    if response.status == 200:
                        if 'application/json' in response.headers.get('Content-Type', ''):
                            return await response.json()
                        return await response.read()

                    # Другие ошибки
                    logger.error(f"Request failed: {response.status}")
                    if attempt < max_retries - 1:
                        delay = base_delay * (attempt + 1)
                        await asyncio.sleep(delay)
                        continue

                    response.raise_for_status()

            except aiohttp.ClientError as e:
                logger.error(f"Client error: {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (attempt + 1)
                    await asyncio.sleep(delay)
                    continue
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise

        raise Exception(f"Failed after {max_retries} attempts")

    async def get_file_names(self) -> List[str]:
        """Получить имена файлов для скачивания"""
        url = f"{self.base_url}/api/files/names"
        try:
            data = await self._request_with_retry("GET", url)
            return data.get("file_names", [])
        except Exception as e:
            logger.error(f"Error getting file names: {e}")
            return []

    async def download_files(self, file_names: List[str]) -> Dict[str, str]:
        """
        Скачать файлы по именам (максимум 3)
        Возвращает словарь {имя_файла: содержимое}
        """
        if len(file_names) > 3:
            raise ValueError("Максимум 3 файла за запрос")

        url = f"{self.base_url}/api/files/download"

        try:
            # Отправляем запрос на скачивание
            async with self.session.post(
                    url,
                    json={"file_names": file_names},
                    headers=self._get_headers()
            ) as response:

                # Обработка ошибок
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logger.warning(f"Rate limit exceeded. Waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return await self.download_files(file_names)

                if response.status == 403:
                    retry_after = int(response.headers.get('Retry-After', 1800))
                    logger.warning(f"Blocked for {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return await self.download_files(file_names)

                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Download error {response.status}: {error_text}")
                    response.raise_for_status()

                # Читаем ZIP-архив
                zip_data = await response.read()

                # Распаковываем
                result = {}
                with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
                    for filename in zip_file.namelist():
                        content = zip_file.read(filename).decode('utf-8')
                        result[filename] = content

                return result

        except Exception as e:
            logger.error(f"Error downloading files: {e}")
            raise

    async def mark_downloaded(self, file_names: List[str]) -> Dict:
        """Отметить файлы как скачанные"""
        url = f"{self.base_url}/api/files/downloaded"
        try:
            data = await self._request_with_retry(
                "POST",
                url,
                json={"file_names": file_names}
            )
            return data
        except Exception as e:
            logger.error(f"Error marking files: {e}")
            return {"marked_now": 0, "already_marked": 0}


# ============ СЕРВИС СКАЧИВАНИЯ ============
async def download_all_files():
    """Основной процесс скачивания всех файлов"""
    if storage.is_downloading:
        logger.info("Download already in progress")
        return

    storage.is_downloading = True
    storage.start_time = datetime.now(pytz.timezone('Asia/Novosibirsk'))
    storage.total_files = 0

    try:
        async with APIClient(API_BASE_URL, CANDIDATE_ID) as client:
            iteration = 0
            while True:
                iteration += 1
                logger.info(f"=== Iteration {iteration} ===")

                # 1. Получаем имена файлов
                file_names = await client.get_file_names()

                # Если список пуст - все файлы скачаны
                if not file_names:
                    logger.info("✅ All files downloaded!")
                    break

                storage.total_files += len(file_names)
                logger.info(f"Got {len(file_names)} file names. Total: {storage.total_files}")

                # 2. Скачиваем файлы по 3 за раз
                for i in range(0, len(file_names), 3):
                    batch = file_names[i:i + 3]
                    logger.info(f"Downloading batch: {batch}")

                    try:
                        # Скачиваем
                        downloaded = await client.download_files(batch)

                        # Сохраняем (исправлено: используем .items())
                        for filename, content in downloaded.items():
                            storage.add_file(filename, content)
                            logger.info(f"✅ Downloaded: {filename}")

                        # Отмечаем как скачанные
                        mark_result = await client.mark_downloaded(batch)
                        logger.info(f"Marked: {mark_result}")

                    except Exception as e:
                        logger.error(f"Error processing batch {batch}: {e}")
                        # Если ошибка, пробуем следующий батч
                        continue

                    # Увеличиваем паузу между запросами
                    delay = random.uniform(DOWNLOAD_DELAY[0], DOWNLOAD_DELAY[1])
                    await asyncio.sleep(delay)

                # Пауза между итерациями
                delay = random.uniform(DOWNLOAD_DELAY[0], DOWNLOAD_DELAY[1])
                await asyncio.sleep(delay)

    except Exception as e:
        logger.error(f"Download error: {e}")
    finally:
        storage.is_downloading = False
        logger.info("Download process finished")


# ============ WEB-ИНТЕРФЕЙС ============
@app.get("/", response_class=HTMLResponse)
async def index():
    """Главная страница"""
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Анализатор файлов</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
                background: #f5f7fa;
                padding: 20px;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                padding: 30px;
                border-radius: 12px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            h1 { color: #2c3e50; margin-bottom: 10px; }
            .subtitle { color: #7f8c8d; margin-bottom: 30px; }

            .section {
                background: #f8f9fa;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 20px;
            }
            .section h2 { color: #2c3e50; font-size: 18px; margin-bottom: 15px; }

            .btn {
                display: inline-block;
                padding: 10px 20px;
                border: none;
                border-radius: 6px;
                font-size: 14px;
                font-weight: 500;
                cursor: pointer;
                transition: all 0.2s;
                margin: 4px;
            }
            .btn-primary { background: #3498db; color: white; }
            .btn-primary:hover { background: #2980b9; }
            .btn-success { background: #2ecc71; color: white; }
            .btn-success:hover { background: #27ae60; }
            .btn-danger { background: #e74c3c; color: white; }
            .btn-danger:hover { background: #c0392b; }
            .btn-warning { background: #f39c12; color: white; }
            .btn-warning:hover { background: #e67e22; }
            .btn:disabled { opacity: 0.6; cursor: not-allowed; }

            .progress-box {
                background: white;
                padding: 15px;
                border-radius: 6px;
                margin-top: 15px;
                border: 1px solid #e0e0e0;
                display: none;
            }
            .progress-box.active { display: block; }
            .progress-bar {
                height: 6px;
                background: #ecf0f1;
                border-radius: 3px;
                margin-top: 10px;
                overflow: hidden;
            }
            .progress-bar .fill {
                height: 100%;
                background: #3498db;
                transition: width 0.3s;
                border-radius: 3px;
            }
            .progress-text { font-size: 14px; color: #555; }

            .file-item {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 10px 15px;
                border-bottom: 1px solid #ecf0f1;
            }
            .file-item:hover { background: #f8f9fa; }
            .file-item .name { font-weight: 500; }
            .file-item .time { color: #7f8c8d; font-size: 13px; }

            .pagination {
                display: flex;
                gap: 5px;
                justify-content: center;
                margin: 15px 0;
                flex-wrap: wrap;
            }
            .pagination .btn { padding: 6px 12px; font-size: 13px; }
            .pagination .btn.active { background: #3498db; color: white; }

            .stats-table {
                width: 100%;
                border-collapse: collapse;
                margin: 15px 0;
                font-size: 14px;
                overflow-x: auto;
                display: block;
            }
            .stats-table th {
                background: #34495e;
                color: white;
                padding: 10px;
                text-align: center;
            }
            .stats-table td {
                padding: 8px;
                text-align: center;
                border-bottom: 1px solid #ecf0f1;
            }
            .stats-table tr:hover td { background: #f8f9fa; }

            .tab-buttons { margin: 20px 0; }
            .tab-content { display: none; }
            .tab-content.active { display: block; }

            .empty-state {
                text-align: center;
                padding: 40px;
                color: #95a5a6;
            }

            .file-controls {
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin: 15px 0;
                padding: 15px;
                background: #f8f9fa;
                border-radius: 6px;
                align-items: center;
            }

            .status-badge {
                display: inline-block;
                padding: 4px 12px;
                border-radius: 12px;
                font-size: 12px;
                font-weight: 500;
            }
            .status-badge.running { background: #f39c12; color: white; }
            .status-badge.stopped { background: #95a5a6; color: white; }
            .status-badge.completed { background: #2ecc71; color: white; }

            @media (max-width: 768px) {
                .container { padding: 15px; }
                .file-controls { flex-direction: column; align-items: stretch; }
                .stats-table { font-size: 12px; }
                .stats-table th, .stats-table td { padding: 5px; }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📁 Анализатор файлов</h1>
            <p class="subtitle">Сервис для скачивания и анализа текстовых файлов</p>

            <!-- Секция скачивания -->
            <div class="section">
                <h2>📥 Скачивание файлов</h2>
                <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap;">
                    <button class="btn btn-success" onclick="startDownload()" id="downloadBtn">
                        🚀 Скачать данные
                    </button>
                    <span id="statusBadge" class="status-badge stopped">Остановлен</span>
                    <span id="startTime" style="color: #7f8c8d; font-size: 14px;">
                        Время старта: —
                    </span>
                </div>

                <div class="progress-box" id="progressBox">
                    <div class="progress-text" id="progressText">
                        Получено 0 названий файлов, скачиваю / скачано 0 из 0
                    </div>
                    <div class="progress-bar">
                        <div class="fill" id="progressFill" style="width: 0%"></div>
                    </div>
                </div>
            </div>

            <!-- Вкладки -->
            <div class="tab-buttons">
                <button class="btn btn-primary" onclick="switchTab('files')">📋 Файлы</button>
                <button class="btn btn-primary" onclick="switchTab('stats')">📊 Статистика</button>
            </div>

            <!-- Вкладка: Файлы -->
            <div class="tab-content active" id="tab-files">
                <div class="file-controls">
                    <button class="btn btn-primary" onclick="loadFiles()">🔄 Обновить</button>
                    <button class="btn btn-warning" onclick="selectAllPage()">✅ Выбрать все на странице</button>
                    <button class="btn btn-warning" onclick="selectAll()">✅ Выбрать все</button>
                    <button class="btn btn-danger" onclick="clearSelection()">❌ Снять выделение</button>
                    <button class="btn btn-success" onclick="calculateStats()">📊 Произвести расчёты</button>
                    <span style="color: #7f8c8d; font-size: 14px; margin-left: auto;">
                        Выбрано: <span id="selectedCount">0</span>
                    </span>
                </div>

                <div id="fileList"></div>
                <div id="pagination"></div>
            </div>

            <!-- Вкладка: Статистика -->
            <div class="tab-content" id="tab-stats">
                <button class="btn btn-primary" onclick="switchTab('files')" style="margin-bottom: 15px;">
                    ⬅️ Назад к файлам
                </button>
                <div id="statsResults">
                    <div class="empty-state">Выберите файлы на вкладке "Файлы" для расчёта статистики</div>
                </div>
            </div>
        </div>

        <script>
            let allFiles = [];
            let selectedFiles = new Set();
            let currentPage = 1;
            const pageSize = 10;
            let updateInterval = null;
            let statsCalculated = false;  // Флаг для предотвращения повторных расчетов
            let allFiles = [];         // Только для текущей страницы
            let allFilesFull = [];     // Все файлы (для "Выбрать все")
            
            // Переключение вкладок (исправлено)
            function switchTab(tab) {
                document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
                document.getElementById(`tab-${tab}`).classList.add('active');

                // Убираем автоматический пересчёт - только ручной по кнопке
                // Статистика показывается только при явном нажатии "Произвести расчёты"
                if (tab === 'stats') {
                    // Если нет выбранных файлов, показываем подсказку
                    if (selectedFiles.size === 0) {
                        document.getElementById('statsResults').innerHTML = 
                            '<div class="empty-state">⚠️ Выберите файлы на вкладке "Файлы" для расчёта статистики</div>';
                    }
                }
            }

            // Запуск скачивания
            async function startDownload() {
                const btn = document.getElementById('downloadBtn');
                btn.disabled = true;
                btn.textContent = '⏳ Скачивание...';

                document.getElementById('startTime').textContent = 
                    `Время старта: ${new Date().toLocaleString('ru-RU', {timeZone: 'Asia/Novosibirsk'})}`;
                document.getElementById('progressBox').classList.add('active');
                document.getElementById('statusBadge').className = 'status-badge running';
                document.getElementById('statusBadge').textContent = '⏳ Выполняется';

                try {
                    const response = await fetch('/api/download', { method: 'POST' });
                    const data = await response.json();

                    if (data.success) {
                        // Обновляем прогресс каждые 2 секунды
                        if (updateInterval) clearInterval(updateInterval);
                        updateInterval = setInterval(updateProgress, 2000);
                    } else {
                        alert(data.message || 'Ошибка запуска скачивания');
                        btn.disabled = false;
                        btn.textContent = '🚀 Скачать данные';
                        document.getElementById('statusBadge').className = 'status-badge stopped';
                        document.getElementById('statusBadge').textContent = 'Остановлен';
                    }
                } catch (error) {
                    console.error('Error:', error);
                    alert('Ошибка при запуске скачивания');
                    btn.disabled = false;
                    btn.textContent = '🚀 Скачать данные';
                    document.getElementById('statusBadge').className = 'status-badge stopped';
                    document.getElementById('statusBadge').textContent = 'Остановлен';
                }
            }

            // Обновление прогресса
            async function updateProgress() {
                try {
                    const response = await fetch('/api/progress');
                    const data = await response.json();

                    const total = data.total || 0;
                    const downloaded = data.downloaded || 0;
                    const inProgress = data.in_progress;

                    document.getElementById('progressText').textContent = 
                        `Получено ${total} названий файлов, скачиваю / скачано ${downloaded} из ${total}`;

                    const percent = total > 0 ? (downloaded / total * 100) : 0;
                    document.getElementById('progressFill').style.width = `${Math.min(percent, 100)}%`;

                    if (!inProgress && total > 0) {
                        clearInterval(updateInterval);
                        document.getElementById('downloadBtn').disabled = false;
                        document.getElementById('downloadBtn').textContent = '🚀 Скачать данные';
                        document.getElementById('statusBadge').className = 'status-badge completed';
                        document.getElementById('statusBadge').textContent = '✅ Завершён';
                        loadFiles();
                        statsCalculated = false; // Сбрасываем флаг при завершении скачивания
                    } else if (!inProgress && total === 0) {
                        // Если процесс завершился без файлов
                        clearInterval(updateInterval);
                        document.getElementById('downloadBtn').disabled = false;
                        document.getElementById('downloadBtn').textContent = '🚀 Скачать данные';
                        document.getElementById('statusBadge').className = 'status-badge stopped';
                        document.getElementById('statusBadge').textContent = 'Остановлен';
                    }
                } catch (error) {
                    console.error('Error updating progress:', error);
                }
            }

            // Загрузка списка файлов
            async function loadFiles(page = 1) {
                currentPage = page;
                try {
                    const response = await fetch(`/api/files?page=${page}&page_size=${pageSize}`);
                    const data = await response.json();
                    allFiles = data.files || [];
                    allFilesFull = data.all_files || allFiles; // Сохраняем все
                    renderFiles(allFiles);
                    renderPagination(data.total, data.page, data.page_size);
                    updateSelectedCount();
                    statsCalculated = false;
                } catch (error) {
                    console.error('Error loading files:', error);
                }
            }

            // Отображение файлов
            function renderFiles(files) {
                const container = document.getElementById('fileList');

                if (!files || files.length === 0) {
                    container.innerHTML = '<div class="empty-state">📭 Нет скачанных файлов</div>';
                    return;
                }

                let html = '';
                files.forEach(file => {
                    const checked = selectedFiles.has(file.name) ? 'checked' : '';
                    html += `
                        <div class="file-item">
                            <span>
                                <input type="checkbox" ${checked} onchange="toggleFile('${file.name.replace(/'/g, "\\'")}')">
                                <span class="name">${file.name}</span>
                            </span>
                            <span class="time">🕐 ${file.time}</span>
                        </div>
                    `;
                });
                container.innerHTML = html;
            }

            // Пагинация
            function renderPagination(total, page, pageSize) {
                const container = document.getElementById('pagination');
                const totalPages = Math.ceil(total / pageSize);

                if (totalPages <= 1) {
                    container.innerHTML = '';
                    return;
                }

                let html = '<div class="pagination">';
                for (let i = 1; i <= totalPages; i++) {
                    const active = i === page ? 'active' : '';
                    html += `<button class="btn ${active}" onclick="loadFiles(${i})">${i}</button>`;
                }
                html += '</div>';
                container.innerHTML = html;
            }

            // Управление выбором
            function toggleFile(name) {
                if (selectedFiles.has(name)) {
                    selectedFiles.delete(name);
                } else {
                    selectedFiles.add(name);
                }
                updateSelectedCount();
                statsCalculated = false; // Сбрасываем флаг при изменении выбора
            }

            function selectAllPage() {
                document.querySelectorAll('.file-item input[type="checkbox"]').forEach(cb => {
                    cb.checked = true;
                    const name = cb.closest('.file-item').querySelector('.name').textContent;
                    selectedFiles.add(name);
                });
                updateSelectedCount();
                statsCalculated = false;
            }

            ffunction selectAll() {
                allFilesFull.forEach(file => selectedFiles.add(file.name)); // ← allFilesFull!
                document.querySelectorAll('.file-item input[type="checkbox"]').forEach(cb => {
                    cb.checked = true;
                });
                updateSelectedCount();
                statsCalculated = false;
            }
                
                // Обновляем счетчик
                updateSelectedCount();
                statsCalculated = false;
            }

            function clearSelection() {
                selectedFiles.clear();
                document.querySelectorAll('.file-item input[type="checkbox"]').forEach(cb => cb.checked = false);
                updateSelectedCount();
                statsCalculated = false;
            }

            function updateSelectedCount() {
                document.getElementById('selectedCount').textContent = selectedFiles.size;
            }

            // Расчет статистики (только по кнопке)
            async function calculateStats() {
                if (selectedFiles.size === 0) {
                    alert('⚠️ Пожалуйста, выберите файлы для расчёта');
                    return;
                }

                try {
                    const response = await fetch('/api/calculate', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ files: Array.from(selectedFiles) })
                    });

                    const data = await response.json();
                    displayStats(data);

                    // Переключаемся на вкладку статистики
                    switchTab('stats');
                    statsCalculated = true;
                } catch (error) {
                    console.error('Error calculating stats:', error);
                    alert('Ошибка при расчете статистики');
                }
            }

            // Отображение статистики
            function displayStats(data) {
                const container = document.getElementById('statsResults');

                if (!data.combined || Object.keys(data.combined).length === 0) {
                    container.innerHTML = '<div class="empty-state">📊 Нет данных для отображения</div>';
                    return;
                }

                let html = '<h3 style="margin: 20px 0 10px;">📊 Общая статистика</h3>';
                html += '<table class="stats-table"><tr><th>Цифра</th>';
                for (let i = 0; i <= 9; i++) {
                    html += `<th>${i}</th>`;
                }
                html += '</tr><tr><td><strong>Всего</strong></td>';
                let total_count = 0;
                for (let i = 0; i <= 9; i++) {
                    const count = data.combined[i] || 0;
                    total_count += count;
                    html += `<td><strong>${count}</strong></td>`;
                }
                html += '</tr>';
                html += `<tr><td colspan="11" style="text-align: right; color: #7f8c8d; font-size: 12px;">
                    Всего цифр: ${total_count}
                </td></tr></table>`;

                if (data.per_file && Object.keys(data.per_file).length > 0) {
                    html += '<h3 style="margin: 20px 0 10px;">📄 Статистика по файлам</h3>';
                    html += '<table class="stats-table"><tr><th>Файл</th>';
                    for (let i = 0; i <= 9; i++) {
                        html += `<th>${i}</th>`;
                    }
                    html += '<th>Сумма</th></tr>';

                    for (const [file, stats] of Object.entries(data.per_file)) {
                        html += `<tr><td><strong>${file}</strong></td>`;
                        let sum = 0;
                        for (let i = 0; i <= 9; i++) {
                            const count = stats[i] || 0;
                            sum += count;
                            html += `<td>${count}</td>`;
                        }
                        html += `<td><strong>${sum}</strong></td>`;
                        html += '</tr>';
                    }
                    html += '</table>';
                }

                container.innerHTML = html;
            }

            // Инициализация
            loadFiles();

            // Проверяем прогресс при загрузке
            setTimeout(updateProgress, 1000);
        </script>
    </body>
    </html>
    """


# ============ API ЭНДПОИНТЫ ============
@app.get("/api/files")
async def get_files(page: int = 1, page_size: int = 10):
    """Получить список скачанных файлов с пагинацией"""
    files = storage.get_files_with_time()
    total = len(files)
    start = (page - 1) * page_size
    end = start + page_size

    return {
        "files": files[start:end],
        "total": total,
        "page": page,
        "page_size": page_size
    }


@app.post("/api/download")
async def start_download(background_tasks: BackgroundTasks):
    """Запустить процесс скачивания в фоне"""
    if storage.is_downloading:
        return {"success": False, "message": "Скачивание уже выполняется"}

    background_tasks.add_task(download_all_files)
    return {"success": True, "message": "Скачивание запущено"}


@app.get("/api/progress")
async def get_progress():
    """Получить прогресс скачивания"""
    return {
        "total": storage.total_files,
        "downloaded": storage.downloaded_count,
        "in_progress": storage.is_downloading
    }


@app.post("/api/calculate")
async def calculate_stats(request: dict):
    """Рассчитать статистику для выбранных файлов"""
    files = request.get("files", [])

    if not files:
        return {"combined": {}, "per_file": {}}

    per_file = {}
    for filename in files:
        stats = storage.get_file_stats(filename)
        if stats:
            per_file[filename] = stats

    combined = storage.get_combined_stats(files)

    return {
        "combined": combined,
        "per_file": per_file
    }


@app.get("/api/stats/all")
async def get_all_stats():
    """Получить статистику по всем файлам"""
    all_files = list(storage.files.keys())
    per_file = {}
    for filename in all_files:
        per_file[filename] = storage.get_file_stats(filename)

    combined = storage.get_combined_stats(all_files)

    return {
        "combined": combined,
        "per_file": per_file,
        "total_files": len(all_files)
    }


@app.post("/api/reset")
async def reset_storage():
    """Сбросить состояние (для тестирования)"""
    storage.clear()
    return {"success": True}


@app.get("/api/status")
async def get_status():
    """Получить статус сервиса"""
    return {
        "is_downloading": storage.is_downloading,
        "total_files": storage.total_files,
        "downloaded_count": storage.downloaded_count,
        "files_stored": len(storage.files)
    }


# ============ ЗАПУСК ============
if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════╗
    ║   Анализатор файлов                  ║
    ║   http://localhost:8000              ║
    ╚═══════════════════════════════════════╝

    📡 API URL: http://91.199.149.128:18001
    🆔 Candidate ID: beluncho.github.io

    Нажми Ctrl+C для остановки
    """)
    uvicorn.run(app, host="0.0.0.0", port=8000)