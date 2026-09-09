import { chromium } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const imagesDir = path.resolve(__dirname, '../../docs/images/yarn');

if (!fs.existsSync(imagesDir)) {
  fs.mkdirSync(imagesDir, { recursive: true });
}

async function run() {
  console.log('🚀 Запуск Chromium (1440x900 @ 2x)...');
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();

  page.on('console', msg => console.log('  [Browser]', msg.text()));
  page.on('pageerror', err => console.error('  [PageError]', err.message));

  console.log('🌐 Переход на http://127.0.0.1:5173 ...');
  await page.goto('http://127.0.0.1:5173');
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(1000);

  // 1. Экран аутентификации
  console.log('📸 1. Скриншот экрана аутентификации...');
  await page.waitForSelector('text=Аутентификация LDAP & Kerberos SSO', { timeout: 5000 });
  await page.waitForTimeout(300);
  await page.screenshot({ path: path.join(imagesDir, '01_login_screen.png'), fullPage: false });
  console.log('   ✅ 01_login_screen.png сохранен');

  // Логин под администратором
  console.log('🔑 Авторизация под admin_user...');
  await page.getByRole('button', { name: /Войти в систему/i }).click();

  // Ждем появления дерева очередей
  console.log('⏳ Ожидание загрузки дерева очередей...');
  await page.getByText('root.prod.spark').waitFor({ state: 'visible', timeout: 10000 });
  await page.waitForTimeout(1000);

  // 2. Главный экран (Dashboard - Процентный режим)
  console.log('📸 2. Скриншот главной панели YARN Explorer...');
  await page.screenshot({ path: path.join(imagesDir, '02_main_dashboard.png'), fullPage: false });
  console.log('   ✅ 02_main_dashboard.png сохранен');

  // 3. Режим абсолютных ресурсов (RAM / CPU)
  console.log('📸 3. Скриншот режима абсолютных ресурсов...');
  await page.getByRole('button', { name: /Абсолютные/i }).click();
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(imagesDir, '03_absolute_resources_mode.png'), fullPage: false });
  console.log('   ✅ 03_absolute_resources_mode.png сохранен');

  // Возврат в %
  await page.getByRole('button', { name: /% Проценты/i }).click();
  await page.waitForTimeout(400);

  // 4. Редактирование очереди (QueueEditDrawer)
  console.log('📸 4. Скриншот панели редактирования очереди root.prod.spark...');
  const sparkRow = page.locator('tr', { hasText: 'root.prod.spark' });
  await sparkRow.getByTitle(/Edit Queue/i).click();
  await page.waitForTimeout(700);
  await page.screenshot({ path: path.join(imagesDir, '04_queue_edit_drawer.png'), fullPage: false });
  console.log('   ✅ 04_queue_edit_drawer.png сохранен');

  // Изменение Capacity на 45% в черновике
  console.log('📝 Изменение емкости на 45% и сохранение в черновик...');
  const capacityInput = page.locator('input[type="number"]').first();
  await capacityInput.fill('45');
  await page.getByRole('button', { name: /Сохранить в черновик/i }).click();
  await page.waitForTimeout(600);

  // 5. Добавление новой очереди (AddQueueModal)
  console.log('📸 5. Скриншот добавления новой очереди...');
  const devRow = page.locator('tr', { hasText: 'root.dev' }).first();
  await devRow.getByTitle(/Add Child Queue/i).click();
  await page.waitForTimeout(600);
  const queueNameInput = page.locator('input[placeholder*="например"]').first();
  if (await queueNameInput.isVisible()) {
    await queueNameInput.fill('feature_analytics');
  }
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(imagesDir, '05_add_queue_modal.png'), fullPage: false });
  console.log('   ✅ 05_add_queue_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(500);

  // 6. Queue Mappings
  console.log('📸 6. Скриншот правил Queue Mappings...');
  await page.getByRole('button', { name: /Queue Mappings/i }).click();
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(imagesDir, '06_queue_mappings_modal.png'), fullPage: false });
  console.log('   ✅ 06_queue_mappings_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(500);

  // 7. DiffPanel (Сравнение изменений)
  console.log('📸 7. Скриншот панели сравнения DiffPanel...');
  await page.locator('button:has-text("Просмотр изменений")').click({ force: true });
  await page.waitForSelector('text=Changes Review', { timeout: 8000 });
  await page.waitForTimeout(700);
  await page.screenshot({ path: path.join(imagesDir, '07_diff_panel.png'), fullPage: false });
  console.log('   ✅ 07_diff_panel.png сохранен');

  // 8. Отправка заявки на согласование (клик внутри DiffPanel)
  console.log('📸 8. Скриншот модального окна заявки на изменение...');
  await page.locator('.fixed').locator('button:has-text("Отправить на согласование")').click({ force: true });
  await page.waitForSelector('text=Заявка на согласование изменений', { timeout: 8000 });
  await page.waitForTimeout(500);
  const titleInput = page.locator('input#cr-title');
  if (await titleInput.isVisible()) {
    await titleInput.fill('Оптимизация квот памяти для ETL Spark');
  }
  const descInput = page.locator('textarea#cr-desc');
  if (await descInput.isVisible()) {
    await descInput.fill('Увеличение гарантированной емкости root.prod.spark до 45% для бесперебойного выполнения ночных пайплайнов.');
  }
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(imagesDir, '08_change_request_modal.png'), fullPage: false });
  console.log('   ✅ 08_change_request_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(500);

  // 9. Центр согласования заявок (ChangeRequestsDrawer)
  console.log('📸 9. Скриншот центра заявок на изменение...');
  await page.locator('header').locator('button:has-text("Заявки на изменение")').click({ force: true });
  await page.waitForTimeout(800);
  await page.screenshot({ path: path.join(imagesDir, '09_change_requests_drawer.png'), fullPage: false });
  console.log('   ✅ 09_change_requests_drawer.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(500);

  // 10. Экспорт XML (XmlExportModal)
  console.log('📸 10. Скриншот экспорта capacity-scheduler.xml...');
  await page.locator('button:has-text("Сгенерировать XML")').click({ force: true });
  await page.waitForSelector('text=capacity-scheduler.xml', { timeout: 8000 });
  await page.waitForTimeout(700);
  await page.screenshot({ path: path.join(imagesDir, '10_xml_export_modal.png'), fullPage: false });
  console.log('   ✅ 10_xml_export_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(400);

  console.log('🎉 ВСЕ 10 СКРИНШОТОВ УСПЕШНО СОЗДАНЫ И СОХРАНЕНЫ!');
  await browser.close();
}

run().catch((err) => {
  console.error('❌ Ошибка генерации скриншотов:', err);
  process.exit(1);
});
