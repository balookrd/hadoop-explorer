export function formatMemory(mb?: number): string {
  if (mb === undefined || mb === null || isNaN(mb)) return '0 MB';
  const abs = Math.abs(mb);
  if (abs >= 1048576) {
    const tb = mb / 1048576;
    const formatted = tb.toFixed(tb % 1 === 0 ? 0 : (Math.abs(tb % 0.1) < 0.01 ? 1 : 2));
    return `${formatted} TB`;
  }
  if (abs >= 1024) {
    const gb = mb / 1024;
    const formatted = gb.toFixed(gb % 1 === 0 ? 0 : (Math.abs(gb % 0.1) < 0.01 ? 1 : 2));
    return `${formatted} GB`;
  }
  return `${Math.round(mb)} MB`;
}

export function formatVcores(cores?: number): string {
  if (cores === undefined || cores === null || isNaN(cores)) return '0 Cores';
  const formatted = cores.toFixed(cores % 1 === 0 ? 0 : (Math.abs(cores % 0.1) < 0.01 ? 1 : 2));
  return `${formatted} Cores`;
}

export function formatMemoryDelta(liveMb: number, draftMb: number): string {
  const deltaMb = draftMb - liveMb;
  if (Math.abs(deltaMb) < 1) return '';
  const sign = deltaMb > 0 ? '+' : '';
  const abs = Math.abs(deltaMb);
  if (abs >= 1048576) {
    const tb = deltaMb / 1048576;
    const formatted = tb.toFixed(tb % 1 === 0 ? 0 : 2);
    return `${sign}${formatted} TB`;
  }
  if (abs >= 1024) {
    const gb = deltaMb / 1024;
    const formatted = gb.toFixed(gb % 1 === 0 ? 0 : 2);
    return `${sign}${formatted} GB`;
  }
  return `${sign}${Math.round(deltaMb)} MB`;
}

export function formatVcoresDelta(liveCores: number, draftCores: number): string {
  const delta = draftCores - liveCores;
  if (Math.abs(delta) < 0.01) return '';
  const sign = delta > 0 ? '+' : '';
  const formatted = delta.toFixed(delta % 1 === 0 ? 0 : 2);
  return `${sign}${formatted} Cores`;
}

export function mbToGb(mb: number): number {
  return Math.round((mb / 1024) * 100) / 100;
}

export function gbToMb(gb: number): number {
  return Math.round(gb * 1024);
}

