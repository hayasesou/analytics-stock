export function ConfidencePill({ confidence }: { confidence: "High" | "Medium" | "Low" }) {
  const cls = confidence.toLowerCase();
  return <span className={`pill ${cls}`}>{confidence}</span>;
}
