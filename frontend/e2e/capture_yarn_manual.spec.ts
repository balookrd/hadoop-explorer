import { test, expect } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';

test.describe('Generate YARN Explorer User Guide Screenshots', () => {
  const imagesDir = path.resolve(process.cwd(), '../docs/images/yarn');

  test.beforeAll(() => {
    if (!fs.existsSync(imagesDir)) {
      fs.mkdirSync(imagesDir, { recursive: true });
    }
  });

  test('captures all core UI views of YARN Explorer', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });

    // Заходим в приложение
    await page.goto('http://localhost:5173');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);

    // 1. Выходим из системы, если залогинены, чтобы снять экран входа
    const userMenuButton = page.locator('header button').filter({ hasText: /@|Админов|Инженеров|Гость/i });
    if (await userMenuButton.isVisible()) {
      await userMenuButton.click();
      await page.waitForTimeout(300);
      const logoutBtn = page.getByRole('button', { name: /Выйти из системы/i });
      if (await logoutBtn.isVisible()) {
        await logoutBtn.click();
        await page.waitForTimeout(500);
      }
    }

    // Скриншот экрана входа
    await expect(page.getByText(/Аутентификация LDAP & Kerberos SSO/i)).toBeVisible();
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(imagesDir, '01_login_screen.png'), fullPage: false });

    // Вход под администратором (admin_user)
    const adminSelectBtn = page.getByText(/Александр Админов/i);
    if (await adminSelectBtn.isVisible()) {
      await adminSelectBtn.click();
    } else {
      await page.locator('#username').fill('admin_user');
      await page.locator('#password').fill('password123');
    }
    await page.getByRole('button', { name: /Войти в систему/i }).click();

    // Ждем полной загрузки дашборда
    await expect(page.getByText('Production Hadoop Cluster')).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('root.prod.spark')).toBeVisible();
    await page.waitForTimeout(1000);

    // 2. Главный экран YARN Explorer (Процентный режим)
    await page.screenshot({ path: path.join(imagesDir, '02_main_dashboard.png'), fullPage: false });

    // 3. Режим отображения ресурсов в абсолютных величинах (GB RAM / vCores)
    await page.getByRole('button', { name: /Абсолютные/i }).click();
    await page.waitForTimeout(500);
    await page.screenshot({ path: path.join(imagesDir, '03_absolute_resources_mode.png'), fullPage: false });

    // Переключаем обратно в проценты
    await page.getByRole('button', { name: /% Проценты/i }).click();
    await page.waitForTimeout(400);

    // 4. Боковая панель детального редактирования очереди (QueueEditDrawer)
    const sparkRow = page.locator('tr', { hasText: 'root.prod.spark' });
    await sparkRow.getByTitle(/Edit Queue/i).click();
    await expect(page.getByText(/Параметры очереди: root.prod.spark|Редактирование очереди/i)).toBeVisible();
    await page.waitForTimeout(600);
    await page.screenshot({ path: path.join(imagesDir, '04_queue_edit_drawer.png'), fullPage: false });

    // Изменяем Capacity с 40 до 45% и сохраняем черновик
    const capacityInput = page.locator('input[type="number"]').first();
    await capacityInput.fill('45');
    await page.getByRole('button', { name: /Сохранить в черновик/i }).click();
    await page.waitForTimeout(500);

    // 5. Модальное окно добавления новой очереди (AddQueueModal)
    const devRow = page.locator('tr', { hasText: 'root.dev' }).first();
    await devRow.getByTitle(/Add Child Queue/i).click();
    await expect(page.getByText(/Добавление дочерней очереди/i)).toBeVisible();
    const queueNameInput = page.locator('input[placeholder*="например"]').first();
    if (await queueNameInput.isVisible()) {
      await queueNameInput.fill('feature_analytics');
    }
    await page.waitForTimeout(500);
    await page.screenshot({ path: path.join(imagesDir, '05_add_queue_modal.png'), fullPage: false });
    await page.getByRole('button', { name: /Отмена/i }).click();
    await page.waitForTimeout(400);

    // 6. Конструктор правил сопоставления очередей (Queue Mappings)
    await page.getByRole('button', { name: /Queue Mappings/i }).click();
    await expect(page.getByText(/Правила сопоставления очередей/i)).toBeVisible();
    await page.waitForTimeout(500);
    await page.screenshot({ path: path.join(imagesDir, '06_queue_mappings_modal.png'), fullPage: false });
    await page.getByRole('button', { name: /Отмена/i }).click();
    await page.waitForTimeout(400);

    // 7. Панель сравнения изменений (DiffPanel)
    await page.getByRole('button', { name: /Просмотр изменений/i }).click();
    await expect(page.getByText(/Сравнение изменений/i)).toBeVisible();
    await page.waitForTimeout(600);
    await page.screenshot({ path: path.join(imagesDir, '07_diff_panel.png'), fullPage: false });

    // 8. Модальное окно отправки заявки на изменение (SubmitChangeRequestModal)
    await page.getByRole('button', { name: /Отправить на согласование/i }).first().click();
    await expect(page.getByText(/Новая заявка на изменение/i)).toBeVisible();
    const titleInput = page.locator('input[placeholder*="Заявка"], input[placeholder*="Например"], input[type="text"]').first();
    if (await titleInput.isVisible()) {
      await titleInput.fill('Оптимизация квот памяти для ETL Spark');
    }
    const descInput = page.locator('textarea');
    if (await descInput.isVisible()) {
      await descInput.fill('Увеличение гарантированной емкости root.prod.spark до 45% для бесперебойного выполнения ночных пайплайнов.');
    }
    await page.waitForTimeout(500);
    await page.screenshot({ path: path.join(imagesDir, '08_change_request_modal.png'), fullPage: false });
    await page.getByRole('button', { name: /Отмена/i }).click();
    await page.waitForTimeout(400);

    // 9. Центр согласования заявок (ChangeRequestsDrawer)
    await page.getByRole('button', { name: /Заявки на изменение/i }).click();
    await expect(page.getByText(/Центр согласования заявок/i)).toBeVisible();
    await page.waitForTimeout(600);
    await page.screenshot({ path: path.join(imagesDir, '09_change_requests_drawer.png'), fullPage: false });
    await page.keyboard.press('Escape');
    await page.waitForTimeout(400);

    // 10. Генерация и экспорт XML (XmlExportModal)
    await page.getByRole('button', { name: /Сгенерировать XML/i }).click();
    await expect(page.getByText(/Экспорт capacity-scheduler.xml/i)).toBeVisible();
    await page.waitForTimeout(600);
    await page.screenshot({ path: path.join(imagesDir, '10_xml_export_modal.png'), fullPage: false });
  });
});
