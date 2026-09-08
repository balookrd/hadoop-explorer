export interface HorizontalResizeOptions {
  initialWidth?: number;
  minWidth?: number;
  maxWidthRatio?: number;
  onResizeEnd?: (width: number) => void;
}

export function createHorizontalResizable(options: HorizontalResizeOptions = {}) {
  const {
    initialWidth = 320,
    minWidth = 180,
    maxWidthRatio = 0.55,
    onResizeEnd,
  } = options;

  let width = $state(initialWidth);
  let isResizing = $state(false);

  function handleMouseMove(e: MouseEvent) {
    if (!isResizing) return;
    const maxW = window.innerWidth * maxWidthRatio;
    const newWidth = Math.max(minWidth, Math.min(maxW, e.clientX));
    width = Math.round(newWidth);
    window.dispatchEvent(new Event('resize'));
  }

  function handleMouseUp() {
    if (!isResizing) return;
    isResizing = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    window.removeEventListener('mousemove', handleMouseMove);
    window.removeEventListener('mouseup', handleMouseUp);
    if (onResizeEnd) {
      onResizeEnd(width);
    }
    window.dispatchEvent(new Event('resize'));
  }

  function startResize(e: MouseEvent) {
    e.preventDefault();
    isResizing = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
  }

  function reset(defaultWidth: number = initialWidth) {
    width = defaultWidth;
    if (onResizeEnd) onResizeEnd(width);
    window.dispatchEvent(new Event('resize'));
  }

  function destroy() {
    window.removeEventListener('mousemove', handleMouseMove);
    window.removeEventListener('mouseup', handleMouseUp);
  }

  return {
    get width() { return width; },
    set width(v: number) { width = v; },
    get isResizing() { return isResizing; },
    startResize,
    reset,
    destroy,
  };
}

export interface VerticalResizeOptions {
  initialPercent?: number;
  minPercent?: number;
  maxPercent?: number;
  containerRef: () => HTMLElement | null;
  onResizeEnd?: (percent: number) => void;
}

export function createVerticalResizable(options: VerticalResizeOptions) {
  const {
    initialPercent = 50,
    minPercent = 15,
    maxPercent = 85,
    containerRef,
    onResizeEnd,
  } = options;

  let percent = $state(initialPercent);
  let isResizing = $state(false);

  function handleMouseMove(e: MouseEvent) {
    if (!isResizing) return;
    const container = containerRef();
    if (!container) return;
    const rect = container.getBoundingClientRect();
    if (rect.height <= 0) return;
    const relativeY = e.clientY - rect.top;
    const newPercent = (relativeY / rect.height) * 100;
    if (newPercent >= minPercent && newPercent <= maxPercent) {
      percent = Math.round(newPercent * 10) / 10;
      window.dispatchEvent(new Event('resize'));
    }
  }

  function handleMouseUp() {
    if (!isResizing) return;
    isResizing = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    window.removeEventListener('mousemove', handleMouseMove);
    window.removeEventListener('mouseup', handleMouseUp);
    if (onResizeEnd) {
      onResizeEnd(percent);
    }
    window.dispatchEvent(new Event('resize'));
  }

  function startResize(e: MouseEvent) {
    e.preventDefault();
    isResizing = true;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
  }

  function reset(defaultPercent: number = initialPercent) {
    percent = defaultPercent;
    if (onResizeEnd) onResizeEnd(percent);
    window.dispatchEvent(new Event('resize'));
  }

  function destroy() {
    window.removeEventListener('mousemove', handleMouseMove);
    window.removeEventListener('mouseup', handleMouseUp);
  }

  return {
    get percent() { return percent; },
    set percent(v: number) { percent = v; },
    get isResizing() { return isResizing; },
    startResize,
    reset,
    destroy,
  };
}
