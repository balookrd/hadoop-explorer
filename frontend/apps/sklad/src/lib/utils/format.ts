export function formatBytes(bytes: number, decimals: number = 2): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

export function formatDate(timestampMs: number): string {
  if (!timestampMs) return '-';
  const date = new Date(timestampMs);
  return new Intl.DateTimeFormat('ru-RU', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

export function formatPermissions(octalPerm: string): string {
  // Octal 755 -> rwxr-xr-x
  const map: Record<string, string> = {
    '0': '---',
    '1': '--x',
    '2': '-w-',
    '3': '-wx',
    '4': 'r--',
    '5': 'r-x',
    '6': 'rw-',
    '7': 'rwx',
  };

  const digits = octalPerm.slice(-3).split('');
  return digits.map(d => map[d] || '---').join('');
}

export function getFileCategory(filename: string): 'text' | 'code' | 'archive' | 'data' | 'image' | 'generic' {
  const ext = filename.split('.').pop()?.toLowerCase() || '';
  if (['txt', 'log', 'md', 'readme'].includes(ext)) return 'text';
  if (['py', 'sh', 'sql', 'js', 'ts', 'html', 'css', 'json', 'yaml', 'yml', 'xml', 'properties'].includes(ext)) return 'code';
  if (['zip', 'tar', 'gz', 'bz2', 'tgz', 'xz'].includes(ext)) return 'archive';
  if (['csv', 'tsv', 'parquet', 'parq', 'avro', 'orc'].includes(ext)) return 'data';
  if (['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp'].includes(ext)) return 'image';
  return 'generic';
}
