import { test, expect } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';

test.describe('Generate HDFS Explorer User Guide Screenshots', () => {
  const imagesDir = path.resolve(process.cwd(), '../docs/images/hdfs');

  test.beforeAll(() => {
    if (!fs.existsSync(imagesDir)) {
      fs.mkdirSync(imagesDir, { recursive: true });
    }
  });

  test('captures core UI views and batch actions of HDFS Explorer', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });

    // 1. Открываем HDFS Explorer на порту демо-стенда 8002
    await page.goto('http://localhost:8002');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);

    // Если есть форма логина, авторизуемся
    const loginButton = page.getByRole('button', { name: /Войти в систему/i });
    if (await loginButton.isVisible()) {
      const adminSelectBtn = page.getByText(/Александр Админов/i);
      if (await adminSelectBtn.isVisible()) {
        await adminSelectBtn.click();
      } else {
        await page.locator('#username').fill('admin_user');
        await page.locator('#password').fill('password123');
      }
      await loginButton.click();
      await page.waitForTimeout(1000);
    }

    // Ждем загрузки файлов
    await expect(page.locator('table')).toBeVisible({ timeout: 10000 });
    await page.waitForTimeout(1000);

    // 2. Главный экран с колонкой чекбоксов
    await page.screenshot({ path: path.join(imagesDir, '02_main_file_browser.png'), fullPage: false });

    // 3. Выбираем чекбоксами несколько строк
    const rowCheckboxes = page.locator('tbody tr input[type="checkbox"]');
    const count = await rowCheckboxes.count();
    if (count > 0) {
      await rowCheckboxes.nth(0).click();
      if (count > 1) {
        await rowCheckboxes.nth(1).click();
      }
      await page.waitForTimeout(500);

      // Снимок с активной плавающей панелью пакетных действий (Batch Action Bar)
      await expect(page.locator('aside')).toBeVisible();
      await page.screenshot({ path: path.join(imagesDir, '11_batch_action_bar.png'), fullPage: false });

      // 4. Открываем модальное окно пакетного удаления
      const deleteBatchBtn = page.locator('aside button').filter({ hasText: /Удалить/i });
      if (await deleteBatchBtn.isVisible()) {
        await deleteBatchBtn.click();
        await page.waitForTimeout(500);
        await expect(page.getByText(/Пакетное удаление объектов/i)).toBeVisible();
        await page.screenshot({ path: path.join(imagesDir, '12_batch_delete_modal.png'), fullPage: false });

        // Закрываем модалку кнопкой Отмена
        const cancelBtn = page.getByRole('button', { name: /Отмена/i });
        if (await cancelBtn.isVisible()) {
          await cancelBtn.click();
        }
      }
    }
  });
});
