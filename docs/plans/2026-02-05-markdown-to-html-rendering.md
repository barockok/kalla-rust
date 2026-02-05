# Markdown-to-HTML Rendering Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Render agent text responses as formatted HTML instead of raw markdown, making the chat interface more user-friendly for non-technical users.

**Architecture:** Add `react-markdown` with `remark-gfm` for GitHub-flavored markdown support. Create a `MarkdownRenderer` component that handles text segments in `ChatMessage`. Apply Tailwind prose classes for clean typography with custom overrides to match the existing design system.

**Tech Stack:** react-markdown, remark-gfm, @tailwindcss/typography, Tailwind CSS prose classes

---

## Task 1: Install Markdown Dependencies

**Files:**
- Modify: `kalla-web/package.json`

**Step 1: Install react-markdown and plugins**

Run:
```bash
cd kalla-web && npm install react-markdown remark-gfm @tailwindcss/typography
```

**Step 2: Verify installation**

Run:
```bash
cd kalla-web && npm ls react-markdown remark-gfm @tailwindcss/typography
```

Expected: All three packages listed with versions

**Step 3: Commit**

```bash
git add kalla-web/package.json kalla-web/package-lock.json
git commit -m "chore: add react-markdown and typography dependencies"
```

---

## Task 2: Configure Tailwind Typography Plugin

**Files:**
- Modify: `kalla-web/postcss.config.mjs` or `kalla-web/tailwind.config.ts` (check which exists)

**Step 1: Check current Tailwind config**

Run:
```bash
ls -la kalla-web/tailwind.config.* kalla-web/postcss.config.* 2>/dev/null
```

Note: Tailwind v4 may use CSS-based configuration. Check `kalla-web/src/app/globals.css` for `@import "tailwindcss"` or `@tailwind` directives.

**Step 2: Enable typography plugin**

For Tailwind v4 with CSS config, add to `globals.css`:

```css
@plugin "@tailwindcss/typography";
```

Or if using JS config, add to plugins array:

```js
plugins: [require('@tailwindcss/typography')]
```

**Step 3: Verify typography classes work**

Run:
```bash
cd kalla-web && npx next build 2>&1 | head -20
```

Expected: Build succeeds without errors

**Step 4: Commit**

```bash
git add kalla-web/src/app/globals.css
git commit -m "feat: enable Tailwind typography plugin"
```

---

## Task 3: Create MarkdownRenderer Component

**Files:**
- Create: `kalla-web/src/components/chat/MarkdownRenderer.tsx`
- Create: `kalla-web/src/__tests__/markdown-renderer.test.tsx`

**Step 1: Write the failing test**

```typescript
// kalla-web/src/__tests__/markdown-renderer.test.tsx
import { render, screen } from '@testing-library/react';
import { MarkdownRenderer } from '@/components/chat/MarkdownRenderer';

describe('MarkdownRenderer', () => {
  test('renders plain text', () => {
    render(<MarkdownRenderer content="Hello world" />);
    expect(screen.getByText('Hello world')).toBeInTheDocument();
  });

  test('renders bold text as strong element', () => {
    render(<MarkdownRenderer content="This is **bold** text" />);
    const strong = screen.getByText('bold');
    expect(strong.tagName).toBe('STRONG');
  });

  test('renders italic text as em element', () => {
    render(<MarkdownRenderer content="This is *italic* text" />);
    const em = screen.getByText('italic');
    expect(em.tagName).toBe('EM');
  });

  test('renders bullet lists as ul/li elements', () => {
    render(<MarkdownRenderer content="- Item 1\n- Item 2\n- Item 3" />);
    const items = screen.getAllByRole('listitem');
    expect(items).toHaveLength(3);
  });

  test('renders numbered lists as ol/li elements', () => {
    render(<MarkdownRenderer content="1. First\n2. Second\n3. Third" />);
    const list = screen.getByRole('list');
    expect(list.tagName).toBe('OL');
  });

  test('renders inline code with code element', () => {
    render(<MarkdownRenderer content="Use `npm install` to install" />);
    const code = screen.getByText('npm install');
    expect(code.tagName).toBe('CODE');
  });

  test('renders links as anchor elements', () => {
    render(<MarkdownRenderer content="Visit [Google](https://google.com)" />);
    const link = screen.getByRole('link', { name: 'Google' });
    expect(link).toHaveAttribute('href', 'https://google.com');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('renders headings correctly', () => {
    render(<MarkdownRenderer content="## Section Title" />);
    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toHaveTextContent('Section Title');
  });

  test('applies custom className', () => {
    const { container } = render(
      <MarkdownRenderer content="Test" className="custom-class" />
    );
    expect(container.firstChild).toHaveClass('custom-class');
  });
});
```

**Step 2: Run test to verify it fails**

Run: `cd kalla-web && npx jest __tests__/markdown-renderer.test.tsx --no-cache`
Expected: FAIL — module not found

**Step 3: Write the implementation**

```typescript
// kalla-web/src/components/chat/MarkdownRenderer.tsx
'use client';

import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { cn } from '@/lib/utils';
import type { Components } from 'react-markdown';

interface MarkdownRendererProps {
  content: string;
  className?: string;
}

// Custom component overrides for styling
const components: Components = {
  // Open links in new tab with security attributes
  a: ({ href, children, ...props }) => (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="text-primary underline underline-offset-2 hover:text-primary/80"
      {...props}
    >
      {children}
    </a>
  ),
  // Style inline code
  code: ({ children, className, ...props }) => {
    // Check if it's a code block (has language class) vs inline code
    const isBlock = className?.includes('language-');
    if (isBlock) {
      return (
        <code
          className={cn(
            'block bg-muted rounded-md p-3 text-xs overflow-x-auto',
            className
          )}
          {...props}
        >
          {children}
        </code>
      );
    }
    return (
      <code
        className="bg-muted px-1.5 py-0.5 rounded text-xs font-mono"
        {...props}
      >
        {children}
      </code>
    );
  },
  // Style code blocks wrapper
  pre: ({ children, ...props }) => (
    <pre className="bg-muted rounded-md overflow-x-auto" {...props}>
      {children}
    </pre>
  ),
  // Reduce heading sizes for chat context
  h1: ({ children, ...props }) => (
    <h1 className="text-lg font-semibold mt-3 mb-1" {...props}>{children}</h1>
  ),
  h2: ({ children, ...props }) => (
    <h2 className="text-base font-semibold mt-2 mb-1" {...props}>{children}</h2>
  ),
  h3: ({ children, ...props }) => (
    <h3 className="text-sm font-semibold mt-2 mb-1" {...props}>{children}</h3>
  ),
  // Style lists
  ul: ({ children, ...props }) => (
    <ul className="list-disc list-inside space-y-0.5 my-1" {...props}>{children}</ul>
  ),
  ol: ({ children, ...props }) => (
    <ol className="list-decimal list-inside space-y-0.5 my-1" {...props}>{children}</ol>
  ),
  li: ({ children, ...props }) => (
    <li className="text-sm" {...props}>{children}</li>
  ),
  // Style paragraphs
  p: ({ children, ...props }) => (
    <p className="my-1 leading-relaxed" {...props}>{children}</p>
  ),
  // Style blockquotes
  blockquote: ({ children, ...props }) => (
    <blockquote
      className="border-l-2 border-muted-foreground/30 pl-3 italic my-2"
      {...props}
    >
      {children}
    </blockquote>
  ),
  // Style horizontal rules
  hr: ({ ...props }) => (
    <hr className="border-muted-foreground/20 my-3" {...props} />
  ),
  // Style tables
  table: ({ children, ...props }) => (
    <div className="overflow-x-auto my-2">
      <table className="min-w-full text-xs border-collapse" {...props}>
        {children}
      </table>
    </div>
  ),
  th: ({ children, ...props }) => (
    <th className="border border-muted-foreground/20 px-2 py-1 bg-muted font-medium text-left" {...props}>
      {children}
    </th>
  ),
  td: ({ children, ...props }) => (
    <td className="border border-muted-foreground/20 px-2 py-1" {...props}>
      {children}
    </td>
  ),
};

export function MarkdownRenderer({ content, className }: MarkdownRendererProps) {
  return (
    <div className={cn('text-sm', className)}>
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
        {content}
      </ReactMarkdown>
    </div>
  );
}
```

**Step 4: Run test to verify it passes**

Run: `cd kalla-web && npx jest __tests__/markdown-renderer.test.tsx --no-cache`
Expected: PASS (9 tests)

**Step 5: Commit**

```bash
git add kalla-web/src/components/chat/MarkdownRenderer.tsx kalla-web/src/__tests__/markdown-renderer.test.tsx
git commit -m "feat: add MarkdownRenderer component for rich text display"
```

---

## Task 4: Integrate MarkdownRenderer into ChatMessage

**Files:**
- Modify: `kalla-web/src/components/chat/ChatMessage.tsx`

**Step 1: Read current implementation**

The file is at `kalla-web/src/components/chat/ChatMessage.tsx`. The `SegmentRenderer` function renders text segments at lines 42-51.

**Step 2: Update SegmentRenderer to use MarkdownRenderer for agent messages**

Replace the text segment rendering (lines 42-51) with:

```typescript
import { MarkdownRenderer } from './MarkdownRenderer';

// In SegmentRenderer function:
if (segment.type === 'text' && segment.content) {
  // Only render markdown for agent messages; keep user messages as plain text
  if (isAgent) {
    return (
      <div className="rounded-lg px-4 py-2 bg-muted text-foreground">
        <MarkdownRenderer content={segment.content} />
      </div>
    );
  }
  return (
    <div className={cn(
      'rounded-lg px-4 py-2 text-sm whitespace-pre-wrap',
      'bg-primary text-primary-foreground'
    )}>
      {segment.content}
    </div>
  );
}
```

**Step 3: Run all tests**

Run: `cd kalla-web && npx jest --no-cache`
Expected: All tests pass

**Step 4: Manual verification**

Run: `cd kalla-web && npm run dev`

Open http://localhost:3000/reconcile and interact with the agent. Verify:
- Bold text renders as bold
- Lists render as bullet/numbered lists
- Code snippets render with monospace styling
- Links are clickable and open in new tabs

**Step 5: Commit**

```bash
git add kalla-web/src/components/chat/ChatMessage.tsx
git commit -m "feat: render agent messages with markdown formatting"
```

---

## Task 5: Final Verification and Cleanup

**Step 1: Run TypeScript check**

Run: `cd kalla-web && npx tsc --noEmit`
Expected: No errors

**Step 2: Run full test suite**

Run: `cd kalla-web && npx jest --no-cache`
Expected: All tests pass

**Step 3: Run production build**

Run: `cd kalla-web && npm run build`
Expected: Build succeeds

**Step 4: Commit any fixes**

```bash
git add -A
git commit -m "fix: resolve any remaining issues from markdown integration"
```

---

## Summary of Changes

| File | Action | Description |
|------|--------|-------------|
| `package.json` | Modify | Add react-markdown, remark-gfm, @tailwindcss/typography |
| `globals.css` | Modify | Enable typography plugin |
| `MarkdownRenderer.tsx` | Create | New component for markdown rendering |
| `markdown-renderer.test.tsx` | Create | Tests for MarkdownRenderer |
| `ChatMessage.tsx` | Modify | Use MarkdownRenderer for agent text segments |

## What Users Will See

**Before:** Raw markdown text like `**bold**`, `- item`, `` `code` ``

**After:** Formatted HTML with:
- **Bold** and *italic* text styled properly
- Bullet and numbered lists with proper indentation
- `inline code` with monospace background
- Code blocks with syntax highlighting background
- Links that are clickable and styled
- Tables with borders and padding
- Blockquotes with left border styling
