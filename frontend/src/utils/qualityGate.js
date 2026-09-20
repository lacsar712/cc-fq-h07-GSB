export function acceptWithoutMean(metrics) {
  return !metrics || metrics.quality_skipped || metrics.mean_quality == null
}

export function meanLabel(metrics) {
  if (acceptWithoutMean(metrics)) return '已跳过'
  return String(metrics.mean_quality)
}
