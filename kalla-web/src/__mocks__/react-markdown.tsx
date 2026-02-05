import React from 'react';
import type { ReactNode } from 'react';

interface ReactMarkdownProps {
  children: string;
  remarkPlugins?: unknown[];
  components?: Record<string, React.ComponentType<unknown>>;
}

// Simple mock that parses basic markdown for testing
function ReactMarkdown({ children, components }: ReactMarkdownProps): ReactNode {
  // Normalize newlines - handle both actual newlines and literal \n sequences
  const normalizedChildren = children.replace(/\\n/g, '\n');

  // Custom link component
  const LinkComponent = components?.a as React.ComponentType<{ href: string; children: ReactNode }> | undefined;
  // Custom code component
  const CodeComponent = components?.code as React.ComponentType<{ children: ReactNode }> | undefined;
  // Custom heading components
  const H1Component = components?.h1 as React.ComponentType<{ children: ReactNode }> | undefined;
  const H2Component = components?.h2 as React.ComponentType<{ children: ReactNode }> | undefined;
  const H3Component = components?.h3 as React.ComponentType<{ children: ReactNode }> | undefined;
  // Custom list components
  const UlComponent = components?.ul as React.ComponentType<{ children: ReactNode }> | undefined;
  const OlComponent = components?.ol as React.ComponentType<{ children: ReactNode }> | undefined;
  const LiComponent = components?.li as React.ComponentType<{ children: ReactNode }> | undefined;
  // Custom paragraph component
  const PComponent = components?.p as React.ComponentType<{ children: ReactNode }> | undefined;

  // Parse the markdown content
  const parseInlineMarkdown = (text: string): ReactNode[] => {
    const parts: ReactNode[] = [];
    let remaining = text;
    let keyIndex = 0;

    while (remaining.length > 0) {
      // Check for bold **text**
      const boldMatch = remaining.match(/^(.*?)\*\*(.+?)\*\*([\s\S]*)/);
      if (boldMatch) {
        if (boldMatch[1]) {
          parts.push(<React.Fragment key={keyIndex++}>{boldMatch[1]}</React.Fragment>);
        }
        parts.push(<strong key={keyIndex++}>{boldMatch[2]}</strong>);
        remaining = boldMatch[3];
        continue;
      }

      // Check for italic *text*
      const italicMatch = remaining.match(/^(.*?)\*(.+?)\*([\s\S]*)/);
      if (italicMatch) {
        if (italicMatch[1]) {
          parts.push(<React.Fragment key={keyIndex++}>{italicMatch[1]}</React.Fragment>);
        }
        parts.push(<em key={keyIndex++}>{italicMatch[2]}</em>);
        remaining = italicMatch[3];
        continue;
      }

      // Check for inline code `text`
      const codeMatch = remaining.match(/^(.*?)`(.+?)`([\s\S]*)/);
      if (codeMatch) {
        if (codeMatch[1]) {
          parts.push(<React.Fragment key={keyIndex++}>{codeMatch[1]}</React.Fragment>);
        }
        if (CodeComponent) {
          parts.push(<CodeComponent key={keyIndex++}>{codeMatch[2]}</CodeComponent>);
        } else {
          parts.push(<code key={keyIndex++}>{codeMatch[2]}</code>);
        }
        remaining = codeMatch[3];
        continue;
      }

      // Check for links [text](url)
      const linkMatch = remaining.match(/^(.*?)\[(.+?)\]\((.+?)\)([\s\S]*)/);
      if (linkMatch) {
        if (linkMatch[1]) {
          parts.push(<React.Fragment key={keyIndex++}>{linkMatch[1]}</React.Fragment>);
        }
        if (LinkComponent) {
          parts.push(
            <LinkComponent key={keyIndex++} href={linkMatch[3]}>
              {linkMatch[2]}
            </LinkComponent>
          );
        } else {
          parts.push(
            <a key={keyIndex++} href={linkMatch[3]}>
              {linkMatch[2]}
            </a>
          );
        }
        remaining = linkMatch[4];
        continue;
      }

      // No more patterns found, add remaining text
      parts.push(<React.Fragment key={keyIndex++}>{remaining}</React.Fragment>);
      break;
    }

    return parts;
  };

  // Check for headings
  const h2Match = normalizedChildren.match(/^## (.+)$/m);
  if (h2Match) {
    const content = h2Match[1];
    if (H2Component) {
      return <H2Component>{content}</H2Component>;
    }
    return <h2>{content}</h2>;
  }

  const h1Match = normalizedChildren.match(/^# (.+)$/m);
  if (h1Match) {
    const content = h1Match[1];
    if (H1Component) {
      return <H1Component>{content}</H1Component>;
    }
    return <h1>{content}</h1>;
  }

  const h3Match = normalizedChildren.match(/^### (.+)$/m);
  if (h3Match) {
    const content = h3Match[1];
    if (H3Component) {
      return <H3Component>{content}</H3Component>;
    }
    return <h3>{content}</h3>;
  }

  // Check for bullet list
  const bulletListMatch = normalizedChildren.match(/^- .+/m);
  if (bulletListMatch) {
    // Split by actual newlines
    const lines = normalizedChildren.split('\n').filter(line => line.trim().length > 0);
    const items = lines.filter(line => line.trim().startsWith('- '));
    const listItems = items.map((item, i) => {
      const content = item.trim().replace(/^- /, '');
      if (LiComponent) {
        return <LiComponent key={i}>{content}</LiComponent>;
      }
      return <li key={i}>{content}</li>;
    });
    if (UlComponent) {
      return <UlComponent>{listItems}</UlComponent>;
    }
    return <ul>{listItems}</ul>;
  }

  // Check for numbered list
  const numberedListMatch = normalizedChildren.match(/^\d+\. .+/m);
  if (numberedListMatch) {
    const items = normalizedChildren.split('\n').filter(line => /^\d+\. /.test(line));
    const listItems = items.map((item, i) => {
      const content = item.replace(/^\d+\. /, '');
      if (LiComponent) {
        return <LiComponent key={i}>{content}</LiComponent>;
      }
      return <li key={i}>{content}</li>;
    });
    if (OlComponent) {
      return <OlComponent>{listItems}</OlComponent>;
    }
    return <ol>{listItems}</ol>;
  }

  // Parse inline content
  const inlineContent = parseInlineMarkdown(normalizedChildren);

  if (PComponent) {
    return <PComponent>{inlineContent}</PComponent>;
  }
  return <p>{inlineContent}</p>;
}

export default ReactMarkdown;
export type { Components } from 'react-markdown';
