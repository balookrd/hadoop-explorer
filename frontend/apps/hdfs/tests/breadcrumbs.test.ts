import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import Breadcrumbs from '../src/lib/components/Breadcrumbs.svelte';
import { explorerStore } from '../src/lib/stores/explorer.svelte';

describe('HDFS Breadcrumbs Component', () => {
  beforeEach(() => {
    explorerStore.currentPath = '/';
    explorerStore.parentPath = null;
  });

  it('renders root path with slash and disabled navigate up button', () => {
    render(Breadcrumbs);
    expect(screen.getByText('/')).toBeInTheDocument();
    const upBtn = screen.getByTitle('На уровень вверх');
    expect(upBtn).toBeDisabled();
  });

  it('enables navigate up button when parentPath exists and calls navigateUp on click', async () => {
    explorerStore.currentPath = '/user/hadoop';
    explorerStore.parentPath = '/user';
    const upSpy = vi.spyOn(explorerStore, 'navigateUp').mockImplementation(() => {});

    render(Breadcrumbs);
    const upBtn = screen.getByTitle('На уровень вверх');
    expect(upBtn).not.toBeDisabled();
    await fireEvent.click(upBtn);

    expect(upSpy).toHaveBeenCalled();
    upSpy.mockRestore();
  });

  it('renders path segments correctly for nested directory', async () => {
    explorerStore.currentPath = '/user/hadoop/datasets';
    render(Breadcrumbs);

    expect(screen.getByText('user')).toBeInTheDocument();
    expect(screen.getByText('hadoop')).toBeInTheDocument();
    expect(screen.getByText('datasets')).toBeInTheDocument();
  });

  it('navigates when clicking on a path segment or root', async () => {
    explorerStore.currentPath = '/user/hadoop/datasets';
    const navSpy = vi.spyOn(explorerStore, 'navigateTo').mockImplementation(() => {});

    render(Breadcrumbs);
    const hadoopSegment = screen.getByText('hadoop');
    await fireEvent.click(hadoopSegment);

    expect(navSpy).toHaveBeenCalledWith('/user/hadoop');

    const rootBtn = screen.getByText('/');
    await fireEvent.click(rootBtn);
    expect(navSpy).toHaveBeenCalledWith('/');

    navSpy.mockRestore();
  });

  it('allows manual path editing and navigates on submit', async () => {
    explorerStore.currentPath = '/user';
    const navSpy = vi.spyOn(explorerStore, 'navigateTo').mockImplementation(() => {});

    render(Breadcrumbs);
    const editBtn = screen.getByTitle('Редактировать путь');
    await fireEvent.click(editBtn);

    const input = screen.getByPlaceholderText('/path/in/hdfs') as HTMLInputElement;
    expect(input).toBeInTheDocument();
    await fireEvent.input(input, { target: { value: '/tmp/output' } });

    const submitBtn = screen.getByTitle('Перейти');
    await fireEvent.click(submitBtn);

    expect(navSpy).toHaveBeenCalledWith('/tmp/output');
    navSpy.mockRestore();
  });

  it('cancels manual path editing on cancel button click', async () => {
    explorerStore.currentPath = '/user';
    render(Breadcrumbs);

    const editBtn = screen.getByTitle('Редактировать путь');
    await fireEvent.click(editBtn);

    expect(screen.getByPlaceholderText('/path/in/hdfs')).toBeInTheDocument();
    const cancelBtn = screen.getByTitle('Отмена');
    await fireEvent.click(cancelBtn);

    expect(screen.queryByPlaceholderText('/path/in/hdfs')).not.toBeInTheDocument();
  });
});
