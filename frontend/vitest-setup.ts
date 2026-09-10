import '@testing-library/jest-dom/vitest';

if (typeof document !== 'undefined') {
  if (!document.queryCommandSupported) {
    (document as any).queryCommandSupported = () => false;
  }
  if (!document.execCommand) {
    (document as any).execCommand = () => false;
  }
}

if (typeof window !== 'undefined') {
  if (!window.matchMedia) {
    window.matchMedia = () =>
      ({
        matches: false,
        media: '',
        onchange: null,
        addListener: () => {},
        removeListener: () => {},
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false
      }) as any;
  }

  if (!window.ResizeObserver) {
    window.ResizeObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    } as any;
  }

  if (!window.IntersectionObserver) {
    window.IntersectionObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    } as any;
  }

  if (!navigator.clipboard) {
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: () => Promise.resolve(),
        readText: () => Promise.resolve('')
      },
      writable: true,
      configurable: true
    });
  }
}
