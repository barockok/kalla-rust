interface SourceStub {
  alias: string;
}

function normalise(text: string): string {
  return text.toLowerCase().replace(/_/g, ' ').trim();
}

/**
 * Detect which source aliases the user is referring to from free-text input.
 * Matches longest alias first. Handles singular/plural by also checking
 * with trailing 's' stripped from both alias and text tokens.
 */
export function detectSourceAliases(
  text: string,
  availableSources: SourceStub[],
): { left: string | null; right: string | null } {
  const normalised = normalise(text);

  // Sort aliases longest-first so "invoices_csv" matches before "invoices"
  const sorted = [...availableSources].sort(
    (a, b) => b.alias.length - a.alias.length,
  );

  const matched: string[] = [];
  let remaining = normalised;

  for (const source of sorted) {
    const aliasWords = normalise(source.alias);

    // Direct substring match
    if (remaining.includes(aliasWords)) {
      matched.push(source.alias);
      remaining = remaining.replace(aliasWords, '');
      continue;
    }

    // Try singularised alias: "payments csv" -> "payment csv"
    const words = aliasWords.split(' ');
    if (words.length > 0) words[0] = words[0].replace(/s$/, '');
    const singularAlias = words.join(' ');
    if (singularAlias !== aliasWords && remaining.includes(singularAlias)) {
      matched.push(source.alias);
      remaining = remaining.replace(singularAlias, '');
      continue;
    }
  }

  return {
    left: matched[0] ?? null,
    right: matched[1] ?? null,
  };
}
