import type { ReactNode } from "react";
import {
  AccordionItem,
  AccordionTrigger,
  AccordionContent,
} from "@/components/ui/accordion";

interface FormSectionProps {
  value: string;
  title: string;
  description?: string;
  children: ReactNode;
}

export function FormSection({ value, title, description, children }: FormSectionProps) {
  return (
    <AccordionItem value={value}>
      <AccordionTrigger className="text-base font-semibold">
        <div className="flex flex-col items-start gap-0.5 text-left">
          <span>{title}</span>
          {description && (
            <span className="text-xs font-normal text-muted-foreground">
              {description}
            </span>
          )}
        </div>
      </AccordionTrigger>
      <AccordionContent>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {children}
        </div>
      </AccordionContent>
    </AccordionItem>
  );
}
