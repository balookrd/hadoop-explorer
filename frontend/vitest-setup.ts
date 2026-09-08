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
}
