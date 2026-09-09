import { chromium } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const imagesDir = path.resolve(__dirname, '../../docs/images/sql');

if (!fs.existsSync(imagesDir)) {
  fs.mkdirSync(imagesDir, { recursive: true });
}

async function run() {
  console.log('🚀 Запуск Chromium для SQL Explorer (1440x900 @ 2x)...');
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
  console.log('📸 1. Скриншот экрана входа SQL Explorer...');
  await page.waitForSelector('text=Аутентификация LDAP & Kerberos SSO', { timeout: 5000 });
  await page.locator('#username').fill('analyst_user');
  await page.locator('#password').fill('password123');
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(imagesDir, '01_login_screen.png'), fullPage: false });
  console.log('   ✅ 01_login_screen.png сохранен');

  // Авторизация под analyst_user
  console.log('🔑 Авторизация под analyst_user...');
  await page.getByRole('button', { name: /Войти в систему/i }).click();

  // Ждем загрузки интерфейса
  console.log('⏳ Ожидание загрузки рабочей студии SQL Explorer...');
  await page.waitForSelector('text=SQL Web Explorer', { timeout: 10000 });
  await page.waitForTimeout(1500);

  // 2. Главный экран рабочей студии
  console.log('📸 2. Скриншот главного экрана рабочей студии (Main Workspace)...');
  await page.screenshot({ path: path.join(imagesDir, '02_main_workspace.png'), fullPage: false });
  console.log('   ✅ 02_main_workspace.png сохранен');

  // 3. Раскрытое дерево каталогов и схемы
  console.log('📸 3. Раскрытие схемы данных и таблицы customer...');
  const customerRow = page.locator('span, div, button').filter({ hasText: /^customer$/ }).first();
  if (await customerRow.isVisible()) {
    await customerRow.click({ force: true });
    await page.waitForTimeout(600);
  }
  await page.screenshot({ path: path.join(imagesDir, '03_catalog_schema_tree.png'), fullPage: false });
  console.log('   ✅ 03_catalog_schema_tree.png сохранен');

  // 4. Выполнение запроса и вывод результатов в таблицу
  console.log('📸 4. Запуск выполнения SQL запроса...');
  await page.getByRole('button', { name: /Выполнить/i }).click();
  await page.waitForSelector('tbody tr', { timeout: 10000 });
  await page.waitForTimeout(1200);
  await page.screenshot({ path: path.join(imagesDir, '04_query_results_grid.png'), fullPage: false });
  console.log('   ✅ 04_query_results_grid.png сохранен');

  // 5. Работа с вкладками (Multi-Tab Editor)
  console.log('📸 5. Добавление новой вкладки редактора...');
  const addTabButton = page.locator('button:has(svg.lucide-plus), button[title*="Новая вкладка"]').first();
  if (await addTabButton.isVisible()) {
    await addTabButton.click({ force: true });
    await page.waitForTimeout(600);
  }
  await page.screenshot({ path: path.join(imagesDir, '05_multi_tab_editor.png'), fullPage: false });
  console.log('   ✅ 05_multi_tab_editor.png сохранен');

  // Переключаемся обратно на первую вкладку
  const firstTab = page.locator('div, button').filter({ hasText: /^Запрос 1$/ }).first();
  if (await firstTab.isVisible()) {
    await firstTab.click({ force: true });
    await page.waitForTimeout(400);
  }

  // 6. Панель истории запросов
  console.log('📸 6. Переход на вкладку Истории запросов...');
  await page.locator('button').filter({ hasText: /История/i }).first().click({ force: true });
  await page.waitForTimeout(800);
  await page.screenshot({ path: path.join(imagesDir, '06_query_history.png'), fullPage: false });
  console.log('   ✅ 06_query_history.png сохранен');

  // 7. Мониторинг очереди кластера
  console.log('📸 7. Переход на вкладку Очереди кластера...');
  await page.locator('button').filter({ hasText: /Очередь/i }).first().click({ force: true });
  await page.waitForTimeout(800);
  await page.screenshot({ path: path.join(imagesDir, '07_cluster_queue_monitor.png'), fullPage: false });
  console.log('   ✅ 07_cluster_queue_monitor.png сохранен');

  // Возврат на вкладку Схема
  await page.locator('button').filter({ hasText: /Схема/i }).first().click({ force: true });
  await page.waitForTimeout(400);

  // 8. ИИ Генератор SQL
  console.log('📸 8. Открытие модального окна ИИ Генератора...');
  await page.locator('button:has-text("ИИ Генератор")').click({ force: true });
  await page.waitForSelector('text=ИИ SQL Ассистент', { timeout: 8000 });
  await page.waitForTimeout(600);
  const promptTextarea = page.locator('#generate-prompt-input');
  if (await promptTextarea.isVisible()) {
    await promptTextarea.fill('Посчитай средний баланс аккаунта (acctbal) и количество клиентов по сегментам рынка (mktsegment) из таблицы tpch.sf1.customer');
  }
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(imagesDir, '08_ai_generator.png'), fullPage: false });
  console.log('   ✅ 08_ai_generator.png сохранен');

  // 9. ИИ Анализ запроса (Linter & Check)
  console.log('📸 9. Переход на вкладку Анализ и замечания...');
  await page.locator('button').filter({ hasText: /Анализ и замечания/i }).first().click({ force: true });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '09_ai_analysis_check.png'), fullPage: false });
  console.log('   ✅ 09_ai_analysis_check.png сохранен');

  // 10. ИИ Объяснение и Оптимизация
  console.log('📸 10. Переход на вкладку Объяснение запроса...');
  await page.locator('button').filter({ hasText: /Объяснение запроса/i }).first().click({ force: true });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '10_ai_explain_optimize.png'), fullPage: false });
  console.log('   ✅ 10_ai_explain_optimize.png сохранен');

  await page.keyboard.press('Escape');
  await page.waitForTimeout(400);

  console.log('🎉 ВСЕ 10 СКРИНШОТОВ SQL EXPLORER УСПЕШНО СОЗДАНЫ!');
  await browser.close();
}

run().catch((err) => {
  console.error('❌ Ошибка генерации скриншотов SQL Explorer:', err);
  process.exit(1);
});
