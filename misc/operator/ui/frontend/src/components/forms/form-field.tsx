import type { ReactNode } from "react";
import { Label } from "@/components/ui/label";

interface FormFieldProps {
  label: string;
  description?: string;
  hint?: string;
  htmlFor?: string;
  children: ReactNode;
}

export function FormField({ label, description, hint, htmlFor, children }: FormFieldProps) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={htmlFor} className="text-sm font-medium">
        {label}
      </Label>
      {description && (
        <p className="text-xs text-muted-foreground">{description}</p>
      )}
      {children}
      {hint && (
        <p className="text-xs text-muted-foreground italic">{hint}</p>
      )}
    </div>
  );
}
