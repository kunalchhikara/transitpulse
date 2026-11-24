import "@testing-library/jest-dom";

// jsdom has no IntersectionObserver. next/link uses one to decide whether
// to prefetch a link once it scrolls into view; without a mock its real
// (Next.js-polyfilled) fallback schedules an async state update via
// requestIdleCallback that lands after the test's render/act scope has
// already finished, producing harmless but noisy "not wrapped in act"
// warnings. A minimal no-op mock keeps output clean without faking any
// behavior this story's tests actually assert on.
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | Document | null = null;
  readonly rootMargin: string = "";
  readonly thresholds: ReadonlyArray<number> = [];
  disconnect() {}
  observe() {}
  takeRecords(): IntersectionObserverEntry[] {
    return [];
  }
  unobserve() {}
}

global.IntersectionObserver = MockIntersectionObserver;
