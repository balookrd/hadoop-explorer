export function formatMemory(mb?: number): string {
  if (mb === undefined || mb === null || isNaN(mb)) return '0 MB';
  if (mb >= 1048576) {
    const tb = mb / 1048576;
    return `${tb % 1 === 0 ? tb.toFixed(0) : tb.toFixed(1)} TB`;
  }
  if (mb >= 1024) {
    const gb = mb / 1024;
    return `${gb % 1 === 0 ? gb.toFixed(0) : gb.toFixed(1)} GB`;
  }
  return `${Math.round(mb)} MB`;
}

export function formatVcores(cores?: number): string {
  if (cores === undefined || cores === null || isNaN(cores)) return '0 Cores';
  return `${Math.round(cores)} Cores`;
}

export function mbToGb(mb: number): number {
  return Math.round((mb / 1024) * 10) / 10;
}

export function gbToMb(gb: number): number {
  return Math.round(gb * 1024);
}
