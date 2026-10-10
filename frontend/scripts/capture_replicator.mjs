import { chromium } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const imagesDir = path.resolve(__dirname, '../../docs/images/replicator');

if (!fs.existsSync(imagesDir)) {
  fs.mkdirSync(imagesDir, { recursive: true });
}

async function run() {
  console.log('🚀 Запуск Chromium для Hadoop gRPC Replicator (1440x900 @ 2x)...');
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();

  page.on('console', msg => {
    const text = msg.text();
    if (!text.includes('Download the Vue Devtools') && !text.includes('vite') && !text.includes('401')) {
      console.log('  [Browser]', text);
    }
  });
  page.on('pageerror', err => console.error('  [PageError]', err.message));

  console.log('🌐 Переход на http://localhost:8005 ...');
  await page.goto('http://localhost:8005');
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(800);

  // 1. Экран аутентификации
  console.log('📸 1. Скриншот экрана аутентификации Replicator...');
  await page.waitForSelector('text=Аутентификация LDAP & Kerberos SSO', { timeout: 8000 });
  await page.locator('#username').fill('admin_user');
  await page.locator('#password').fill('password123');
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(imagesDir, '01_login_screen.png'), fullPage: false });
  console.log('   ✅ 01_login_screen.png сохранен');

  // Вход в систему под admin_user
  console.log('🔑 Авторизация под admin_user...');
  await page.getByRole('button', { name: /Войти в систему/i }).click();
  await page.waitForSelector('text=Всего задач', { timeout: 10000 });
  await page.waitForTimeout(1000);

  // 2. Главная панель управления HDFS репликацией
  console.log('📸 2. Скриншот главной панели HDFS Replication...');
  await page.screenshot({ path: path.join(imagesDir, '02_main_dashboard.png'), fullPage: false });
  console.log('   ✅ 02_main_dashboard.png сохранен');

  // 3. Модальное окно создания новой задачи репликации HDFS
  console.log('📸 3. Скриншот модального окна создания HDFS задачи...');
  const newJobBtn = page.getByRole('button', { name: /Новая задача репликации/i }).first();
  await newJobBtn.click();
  await page.waitForSelector('text=Новая задача репликации', { timeout: 6000 });
  await page.waitForTimeout(500);

  // Заполняем демонстрационные поля
  const srcInput = page.locator('input[placeholder*="/data/production/events"]');
  if (await srcInput.isVisible()) {
    await srcInput.fill('/data/analytics/warehouse/orders_2026');
  }
  const dstInput = page.locator('input[placeholder*="/backup/mirror/events"]');
  if (await dstInput.isVisible()) {
    await dstInput.fill('/backup/analytics/warehouse/orders_2026');
  }
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(imagesDir, '03_create_job_modal.png'), fullPage: false });
  console.log('   ✅ 03_create_job_modal.png сохранен');

  // Закрываем модалку через кнопку «Отмена»
  await page.locator('.fixed button:has-text("Отмена")').click({ force: true });
  await page.waitForTimeout(700);

  // 4. Раздел «Топология ЦОД и Полоса»
  console.log('📸 4. Скриншот раздела «Топология ЦОД и Полоса»...');
  await page.locator('nav button:has-text("Топология ЦОД и Полоса")').click();
  await page.waitForSelector('text=Топология ЦОД и Управление полосой WAN', { timeout: 8000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '04_topology_bandwidth.png'), fullPage: false });
  console.log('   ✅ 04_topology_bandwidth.png сохранен');

  // Возвращаемся на вкладку «HDFS Replication» для открытия истории
  await page.locator('nav button:has-text("HDFS Replication")').click();
  await page.waitForTimeout(500);

  // 5. Модальное окно истории запусков задачи
  console.log('📸 5. Скриншот истории запусков задачи (Job Runs History)...');
  const historyBtn = page.locator('button[title*="История"]').first();
  if (await historyBtn.isVisible()) {
    await historyBtn.click({ force: true });
    await page.waitForSelector('text=История запусков задачи', { timeout: 6000 });
    await page.waitForTimeout(800);
    await page.screenshot({ path: path.join(imagesDir, '05_job_history_modal.png'), fullPage: false });
    console.log('   ✅ 05_job_history_modal.png сохранен');

    // Закрываем модалку через кнопку «Закрыть»
    await page.locator('.fixed button:has-text("Закрыть")').click({ force: true });
    await page.waitForTimeout(700);
  }

  // 6. Раздел «HMS Replication» (Hive Metastore CDC)
  console.log('📸 6. Скриншот раздела «HMS Replication»...');
  await page.locator('nav button:has-text("HMS Replication")').click();
  await page.waitForSelector('text=Всего схем', { timeout: 8000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '06_hms_replication_dashboard.png'), fullPage: false });
  console.log('   ✅ 06_hms_replication_dashboard.png сохранен');

  // 7. Модальное окно создания CDC репликации схемы HMS
  console.log('📸 7. Скриншот создания репликации схемы HMS...');
  const newHmsBtn = page.locator('button:has-text("Новая репликация схемы")').first();
  await newHmsBtn.click();
  await page.waitForSelector('text=Новая репликация схемы Hive Metastore', { timeout: 6000 });
  await page.waitForTimeout(500);
  const srcDbInput = page.locator('input[placeholder*="например, analytics_db"]');
  if (await srcDbInput.isVisible()) {
    await srcDbInput.fill('analytics_orders');
  }
  const dstDbInput = page.locator('input[placeholder*="например, analytics_db_backup"]');
  if (await dstDbInput.isVisible()) {
    await dstDbInput.fill('analytics_orders_replica');
  }
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(imagesDir, '07_create_hms_modal.png'), fullPage: false });
  console.log('   ✅ 07_create_hms_modal.png сохранен');

  // Закрываем модалку через кнопку «Отмена»
  await page.locator('.fixed button:has-text("Отмена")').click({ force: true });
  await page.waitForTimeout(700);

  // 8. Раздел «Disaster Recovery & Failover»
  console.log('📸 8. Скриншот раздела «Disaster Recovery»...');
  await page.locator('nav button:has-text("Disaster Recovery")').click();
  await page.waitForSelector('text=Disaster Recovery & Failover Hub', { timeout: 8000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '08_disaster_recovery_dashboard.png'), fullPage: false });
  console.log('   ✅ 08_disaster_recovery_dashboard.png сохранен');

  // 9. Модальное окно экстренного останова (Kill-Switch)
  console.log('📸 9. Скриншот модального окна Kill-Switch...');
  const killSwitchBtn = page.locator('button:has-text("Kill-Switch")').first();
  await killSwitchBtn.click();
  await page.waitForSelector('text=Экстренный останов репликации (Kill-Switch)', { timeout: 6000 });
  await page.waitForTimeout(400);
  const reasonInput = page.locator('input[placeholder*="причина"], input[type="text"]').last();
  if (await reasonInput.isVisible()) {
    await reasonInput.fill('Плановые комплексные учения Disaster Recovery / Имитация аварии DC1');
  }
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(imagesDir, '09_emergency_kill_switch_modal.png'), fullPage: false });
  console.log('   ✅ 09_emergency_kill_switch_modal.png сохранен');

  // Активируем Kill-Switch для фиксации состояния изоляции
  console.log('🛑 Активация Kill-Switch для демонстрации состояния изоляции...');
  const confirmStopBtn = page.locator('button:has-text("Подтвердить экстренный останов")');
  if (await confirmStopBtn.isVisible()) {
    await confirmStopBtn.click();
    await page.waitForTimeout(1500);
  }

  // 10. Состояние дашборда при активной изоляции (Fenced Cluster)
  console.log('📸 10. Скриншот дашборда при активной изоляции DC1 (Fenced State)...');
  await page.waitForSelector('text=ПОДАВЛЕН (FENCED)', { timeout: 8000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '10_disaster_recovery_fenced_state.png'), fullPage: false });
  console.log('   ✅ 10_disaster_recovery_fenced_state.png сохранен');

  // 11. Модальное окно снятия изоляции / откат (Rollback Modal с защитой от Split-Brain)
  console.log('📸 11. Скриншот модального окна снятия изоляции (Unfence Modal)...');
  const rollbackBtn = page.getByRole('button', { name: /Снять изоляцию/i }).first();
  await rollbackBtn.click();
  await page.waitForSelector('text=Снятие изоляции и откат Kill-Switch', { timeout: 6000 });
  await page.waitForSelector('text=Защита от перезаписи данных (Split-Brain Protection)', { timeout: 6000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(imagesDir, '11_rollback_unfence_modal.png'), fullPage: false });
  console.log('   ✅ 11_rollback_unfence_modal.png сохранен');

  // Закрываем модалку через «Отмена»
  await page.locator('.fixed button:has-text("Отмена")').click({ force: true });
  await page.waitForTimeout(500);

  // 12. Модальное окно мастера обратной репликации (Reverse Replication)
  console.log('📸 12. Скриншот модального окна Reverse Replication (DC2 ➔ DC1)...');
  const reverseBtn = page.locator('button:has-text("Reverse Replication")').first();
  await reverseBtn.click();
  await page.waitForSelector('text=Запуск обратной репликации (Reverse Replication)', { timeout: 6000 });
  await page.waitForTimeout(400);
  const reverseConfirmInput = page.locator('input[placeholder="REVERSE"]');
  if (await reverseConfirmInput.isVisible()) {
    await reverseConfirmInput.fill('REVERSE');
  }
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(imagesDir, '12_reverse_replication_modal.png'), fullPage: false });
  console.log('   ✅ 12_reverse_replication_modal.png сохранен');

  // Закрываем модалку через кнопку «Отмена»
  await page.locator('.fixed button:has-text("Отмена")').click({ force: true });
  await page.waitForTimeout(500);

  // Закрываем модалку через кнопку «Отмена»
  await page.locator('.fixed button:has-text("Отмена")').click({ force: true });
  await page.waitForTimeout(500);

  console.log('🎉 ВСЕ 12 СКРИНШОТОВ HADOOP REPLICATOR УСПЕШНО СОЗДАНЫ!');
  await browser.close();
}

run().catch((err) => {
  console.error('❌ Ошибка при генерации скриншотов:', err);
  process.exit(1);
});
