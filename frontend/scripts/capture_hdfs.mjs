import { chromium } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const imagesDir = path.resolve(__dirname, '../../docs/images/hdfs');

if (!fs.existsSync(imagesDir)) {
  fs.mkdirSync(imagesDir, { recursive: true });
}

async function run() {
  console.log('🚀 Запуск Chromium для HDFS Explorer (1440x900 @ 2x)...');
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();

  page.on('console', msg => console.log('  [Browser]', msg.text()));
  page.on('pageerror', err => console.error('  [PageError]', err.message));

  console.log('🌐 Переход на http://127.0.0.1:3000 ...');
  await page.goto('http://127.0.0.1:3000');
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(800);

  // 1. Экран аутентификации
  console.log('📸 1. Скриншот экрана входа HDFS Explorer...');
  await page.waitForSelector('text=Аутентификация LDAP & Kerberos SSO', { timeout: 5000 });
  await page.locator('#username').fill('admin');
  await page.locator('#password').fill('password123');
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(imagesDir, '01_login_screen.png'), fullPage: false });
  console.log('   ✅ 01_login_screen.png сохранен');

  // Авторизация под admin
  console.log('🔑 Авторизация под admin...');
  await page.getByRole('button', { name: /Войти в систему/i }).click();

  // Ждем загрузки интерфейса
  console.log('⏳ Ожидание загрузки файлового менеджера...');
  await page.waitForSelector('text=HDFS Explorer', { timeout: 10000 });
  await page.waitForTimeout(1000);

  // Переходим в корень "/" через хлебные крошки (кнопка со слешем '/')
  console.log('🏠 Переход в корень кластера / ...');
  await page.locator('nav button').filter({ hasText: '/' }).first().click({ force: true });
  await page.waitForSelector('tbody tr:has-text("data")', { timeout: 8000 });
  await page.waitForTimeout(800);

  // 2. Главный экран файлового менеджера (Root Directory)
  console.log('📸 2. Скриншот главного экрана файлового менеджера HDFS...');
  await page.screenshot({ path: path.join(imagesDir, '02_main_file_browser.png'), fullPage: false });
  console.log('   ✅ 02_main_file_browser.png сохранен');

  // Переход в папку /data
  console.log('📁 Переход в папку /data ...');
  const dataRow = page.locator('tbody tr').filter({ hasText: 'data' }).first();
  await dataRow.locator('button').first().click({ force: true });
  await page.waitForSelector('tbody tr:has-text("metrics.csv")', { timeout: 8000 });
  await page.waitForTimeout(800);

  // 3. Просмотр содержимого директории /data
  console.log('📸 3. Скриншот содержимого директории /data...');
  await page.screenshot({ path: path.join(imagesDir, '03_directory_view.png'), fullPage: false });
  console.log('   ✅ 03_directory_view.png сохранен');

  // 4. Предпросмотр CSV файла (табличный вид)
  console.log('📸 4. Скриншот предпросмотра CSV таблицы (metrics.csv)...');
  const metricsRow = page.locator('tbody tr').filter({ hasText: 'metrics.csv' }).first();
  await metricsRow.locator('button[title*="Предпросмотр"]').click({ force: true });
  await page.waitForSelector('th:has-text("timestamp"), th:has-text("cpu_usage")', { timeout: 8000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '04_preview_csv_table.png'), fullPage: false });
  console.log('   ✅ 04_preview_csv_table.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(600);

  // 5. Предпросмотр JSON файла
  console.log('📸 5. Скриншот предпросмотра JSON файла (events.json)...');
  const eventsRow = page.locator('tbody tr').filter({ hasText: 'events.json' }).first();
  await eventsRow.locator('button[title*="Предпросмотр"]').click({ force: true });
  await page.waitForSelector('pre', { timeout: 8000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '05_preview_json_code.png'), fullPage: false });
  console.log('   ✅ 05_preview_json_code.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(600);

  // 6. Модальное окно загрузки файлов (UploadModal)
  console.log('📸 6. Скриншот модального окна загрузки файла (UploadModal)...');
  await page.locator('button:has-text("Загрузить")').click({ force: true });
  await page.waitForSelector('text=Загрузка в HDFS', { timeout: 8000 });
  await page.waitForTimeout(700);
  await page.screenshot({ path: path.join(imagesDir, '06_upload_modal.png'), fullPage: false });
  console.log('   ✅ 06_upload_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(600);

  // 7. Модальное окно создания папки (MkdirModal)
  console.log('📸 7. Скриншот создания новой папки (MkdirModal)...');
  await page.locator('button:has-text("Новая папка")').click({ force: true });
  await page.waitForSelector('text=Новая директория', { timeout: 8000 });
  await page.waitForTimeout(400);
  const dirNameInput = page.locator('#dirName');
  if (await dirNameInput.isVisible()) {
    await dirNameInput.fill('analytics_2026');
  }
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(imagesDir, '07_mkdir_modal.png'), fullPage: false });
  console.log('   ✅ 07_mkdir_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(600);

  // 8. Модальное окно переименования (RenameModal)
  console.log('📸 8. Скриншот переименования файла (RenameModal)...');
  const renameTarget = page.locator('tbody tr').filter({ hasText: 'events.json' }).first();
  await renameTarget.locator('button[title*="Переименовать"]').click({ force: true });
  await page.waitForSelector('text=Переименовать / Переместить', { timeout: 8000 });
  await page.waitForTimeout(400);
  const renameInput = page.locator('#newName');
  if (await renameInput.isVisible()) {
    await renameInput.fill('events_backup_2026.json');
  }
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(imagesDir, '08_rename_modal.png'), fullPage: false });
  console.log('   ✅ 08_rename_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(600);

  // 9. Модальное окно межкластерного копирования (CrossClusterCopyModal)
  console.log('📸 9. Скриншот межкластерного копирования (CrossClusterCopyModal)...');
  const copyTarget = page.locator('tbody tr').filter({ hasText: 'metrics.csv' }).first();
  await copyTarget.locator('button[title*="Скопировать в другой кластер"]').click({ force: true });
  await page.waitForSelector('text=Копирование в другой кластер', { timeout: 8000 });
  await page.waitForTimeout(700);
  await page.screenshot({ path: path.join(imagesDir, '09_cross_cluster_copy_modal.png'), fullPage: false });
  console.log('   ✅ 09_cross_cluster_copy_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(600);

  // 10. Модальное окно удаления (DeleteModal)
  console.log('📸 10. Скриншот удаления файла (DeleteModal)...');
  const deleteTarget = page.locator('tbody tr').filter({ hasText: 'events.json' }).first();
  await deleteTarget.locator('button[title*="Удалить"]').click({ force: true });
  await page.waitForSelector('text=Подтверждение удаления', { timeout: 8000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(imagesDir, '10_delete_modal.png'), fullPage: false });
  console.log('   ✅ 10_delete_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(500);

  console.log('🎉 ВСЕ 10 СКРИНШОТОВ HDFS EXPLORER УСПЕШНО СОЗДАНЫ!');
  await browser.close();
}

run().catch((err) => {
  console.error('❌ Ошибка генерации скриншотов HDFS:', err);
  process.exit(1);
});
