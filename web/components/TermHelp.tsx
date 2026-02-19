"use client";

import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

type HelpSection = {
  label: string;
  text: string;
};

type Props = {
  term: string;
  sections: readonly HelpSection[];
};

type PopoverPosition = {
  top: number;
  left: number;
  width: number;
};

export function TermHelp({ term, sections }: Props) {
  const [open, setOpen] = useState(false);
  const [mounted, setMounted] = useState(false);
  const [position, setPosition] = useState<PopoverPosition | null>(null);
  const rootRef = useRef<HTMLSpanElement | null>(null);
  const buttonRef = useRef<HTMLButtonElement | null>(null);
  const popoverRef = useRef<HTMLSpanElement | null>(null);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (!open) {
      return;
    }

    const updatePosition = () => {
      const button = buttonRef.current;
      if (!button) {
        return;
      }
      const rect = button.getBoundingClientRect();
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      const margin = 8;
      const width = Math.min(360, vw - margin * 2);
      const popoverEstimatedHeight = 260;

      const left = Math.min(Math.max(rect.left, margin), Math.max(margin, vw - width - margin));
      let top = rect.bottom + 8;
      if (top + popoverEstimatedHeight > vh - margin) {
        top = Math.max(margin, rect.top - popoverEstimatedHeight - 8);
      }
      setPosition({ top, left, width });
    };

    updatePosition();

    const onDocClick = (event: MouseEvent) => {
      const target = event.target as Node | null;
      if (!target) {
        return;
      }
      const inButton = buttonRef.current?.contains(target) ?? false;
      const inPopover = popoverRef.current?.contains(target) ?? false;
      if (!inButton && !inPopover) {
        setOpen(false);
      }
    };
    const onEsc = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpen(false);
      }
    };

    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onEsc);
    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onEsc);
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
    };
  }, [open]);

  return (
    <span className="term-help" ref={rootRef}>
      <button
        type="button"
        className="term-help-btn"
        aria-label={`${term} の説明を表示`}
        ref={buttonRef}
        onClick={() => setOpen((v) => !v)}
      >
        ?
      </button>
      {open && mounted
        ? createPortal(
            <span
              ref={popoverRef}
              className="term-help-popover term-help-popover-fixed"
              role="dialog"
              aria-label={`${term} の説明`}
              style={position ? { top: position.top, left: position.left, width: position.width } : undefined}
            >
              <strong className="term-help-title">{term}</strong>
              {sections.map((section) => (
                <span key={`${term}-${section.label}`} className="term-help-row">
                  <span className="term-help-key">{section.label}:</span> {section.text}
                </span>
              ))}
            </span>,
            document.body
          )
        : null}
    </span>
  );
}
