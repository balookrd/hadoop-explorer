import { chromium } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const imagesDir = path.resolve(__dirname, '../../docs/images/spark');

if (!fs.existsSync(imagesDir)) {
  fs.mkdirSync(imagesDir, { recursive: true });
}

async function run() {
  console.log('🚀 Запуск Chromium для Spark Explorer (1440x900 @ 2x)...');
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
  console.log('📸 1. Скриншот экрана входа Spark Explorer...');
  await page.waitForSelector('text=Аутентификация LDAP & Kerberos SSO', { timeout: 8000 });
  await page.locator('#username').fill('de_user');
  await page.locator('#password').fill('password123');
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(imagesDir, '01_login_screen.png'), fullPage: false });
  console.log('   ✅ 01_login_screen.png сохранен');

  // Авторизация под de_user
  console.log('🔑 Авторизация под de_user...');
  await page.getByRole('button', { name: /Войти в систему/i }).click();

  // Ждем загрузки интерфейса
  console.log('⏳ Ожидание загрузки рабочей студии Spark Explorer...');
  await page.waitForSelector('text=Spark Explorer', { timeout: 10000 });
  await page.waitForTimeout(1500);

  // 2. Главный экран рабочей студии
  console.log('📸 2. Скриншот главного экрана рабочей студии (Main Workspace)...');
  await page.screenshot({ path: path.join(imagesDir, '02_main_workspace.png'), fullPage: false });
  console.log('   ✅ 02_main_workspace.png сохранен');

  // 3. Раскрытое дерево каталогов метаданных
  console.log('📸 3. Раскрытие дерева каталога метаданных и таблицы customers...');
  const customersTable = page.locator('span, div, button').filter({ hasText: /^customers$/ }).first();
  if (await customersTable.isVisible()) {
    await customersTable.click({ force: true });
    await page.waitForTimeout(600);
  }
  await page.screenshot({ path: path.join(imagesDir, '03_catalog_tree.png'), fullPage: false });
  console.log('   ✅ 03_catalog_tree.png сохранен');

  // 4. Выполнение PySpark скрипта
  console.log('📸 4. Запуск PySpark скрипта и предпросмотр результатов...');
  await page.locator('button:has-text("PySpark")').first().click({ force: true });
  await page.waitForTimeout(300);
  await page.evaluate(() => {
    const models = window.monaco?.editor?.getModels();
    if (models && models.length > 0) {
      models[0].setValue('from pyspark.sql import functions as F\n\n# Загрузка и анализ витрины клиентов\ndf = spark.read.table("customers")\ndf_filtered = df.filter(F.col("balance") > 5000)\ndf_filtered.show(20)\n');
    }
  });
  await page.waitForTimeout(400);
  const runBtn = page.locator('button:has-text("Выполнить")').first();
  await runBtn.click({ force: true });
  await page.waitForSelector('text=Алексей', { timeout: 10000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(imagesDir, '04_pyspark_execution_results.png'), fullPage: false });
  console.log('   ✅ 04_pyspark_execution_results.png сохранен');

  // 5. Вкладка логов выполнения (Logs)
  console.log('📸 5. Переключение на вкладку консоли и логов Spark...');
  const logsTabBtn = page.locator('button:has-text("Консоль и логи Spark")').first();
  if (await logsTabBtn.isVisible()) {
    await logsTabBtn.click({ force: true });
    await page.waitForTimeout(600);
  }
  await page.screenshot({ path: path.join(imagesDir, '08_execution_logs.png'), fullPage: false });
  console.log('   ✅ 08_execution_logs.png сохранен');

  // Возврат на вкладку таблицы
  const tableTabBtn = page.locator('button:has-text("Таблица DataFrame")').first();
  if (await tableTabBtn.isVisible()) {
    await tableTabBtn.click({ force: true });
    await page.waitForTimeout(400);
  }

  // 6. Модальное окно конфигурации сессии (SessionConfigModal)
  console.log('📸 6. Модальное окно настроек сессии Spark (SessionConfigModal)...');
  const settingsBtn = page.locator('button[title*="Параметры сессии Spark"]').first();
  await settingsBtn.click({ force: true });
  await page.waitForSelector('text=Параметры сессии Spark', { timeout: 5000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(imagesDir, '07_session_config_modal.png'), fullPage: false });
  console.log('   ✅ 07_session_config_modal.png сохранен');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(500);

  // 7. Переключение на Scala Spark
  console.log('📸 7. Переключение на Scala Spark...');
  const scalaBtn = page.locator('button:has-text("Scala Spark")').first();
  await scalaBtn.click({ force: true });
  await page.waitForTimeout(500);

  // Устанавливаем Scala код в Monaco редакторе
  await page.evaluate(() => {
    const models = window.monaco?.editor?.getModels();
    if (models && models.length > 0) {
      models[0].setValue('// Scala Spark аналитический скрипт\nval df = spark.read.table("customers")\nval res = df.filter($"balance" > 10000)\nres.show(50, false)\n');
    }
  });
  await page.waitForTimeout(400);
  await page.locator('button:has-text("Выполнить")').first().click({ force: true });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '05_scala_spark_editor.png'), fullPage: false });
  console.log('   ✅ 05_scala_spark_editor.png сохранен');

  // 8. Переключение на Spark SQL
  console.log('📸 8. Переключение на Spark SQL...');
  const sqlBtn = page.locator('button:has-text("Spark SQL")').first();
  await sqlBtn.click({ force: true });
  await page.waitForTimeout(500);

  // Устанавливаем SQL код в Monaco редакторе
  await page.evaluate(() => {
    const models = window.monaco?.editor?.getModels();
    if (models && models.length > 0) {
      models[0].setValue('-- Spark SQL интерактивный запрос\nSELECT cust_id, first_name, last_name, balance\nFROM customers\nWHERE balance > 10000\nORDER BY balance DESC\nLIMIT 50;\n');
    }
  });
  await page.waitForTimeout(400);
  await page.locator('button:has-text("Выполнить")').first().click({ force: true });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(imagesDir, '06_spark_sql_query.png'), fullPage: false });
  console.log('   ✅ 06_spark_sql_query.png сохранен');

  // 9. Многовкладочный режим (создание новых вкладок)
  console.log('📸 9. Создание дополнительных вкладок (Multitab)...');
  const addTabBtn = page.locator('button[title="Новый скрипт"]').first();
  if (await addTabBtn.isVisible()) {
    await addTabBtn.click({ force: true });
    await page.waitForTimeout(400);
    await addTabBtn.click({ force: true });
    await page.waitForTimeout(600);
  }
  await page.screenshot({ path: path.join(imagesDir, '09_multitab_workspace.png'), fullPage: false });
  console.log('   ✅ 09_multitab_workspace.png сохранен');

  console.log('🎉 ВСЕ 9 СКРИНШОТОВ SPARK EXPLORER УСПЕШНО СОЗДАНЫ!');
  await browser.close();
}

run().catch((err) => {
  console.error('❌ Ошибка генерации скриншотов Spark:', err);
  process.exit(1);
});
