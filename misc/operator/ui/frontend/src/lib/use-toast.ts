import * as React from "react";

type ToastVariant = "default" | "destructive";

interface Toast {
  id: string;
  title: string;
  description?: string;
  variant?: ToastVariant;
}

interface ToastState {
  toasts: Toast[];
}

type Action =
  | { type: "ADD"; toast: Toast }
  | { type: "DISMISS"; id: string };

let count = 0;
function genId() {
  count = (count + 1) % Number.MAX_SAFE_INTEGER;
  return count.toString();
}

const listeners: Array<(state: ToastState) => void> = [];
let memoryState: ToastState = { toasts: [] };

function dispatch(action: Action) {
  switch (action.type) {
    case "ADD":
      memoryState = {
        toasts: [...memoryState.toasts, action.toast],
      };
      break;
    case "DISMISS":
      memoryState = {
        toasts: memoryState.toasts.filter((t) => t.id !== action.id),
      };
      break;
  }
  listeners.forEach((l) => l(memoryState));
}

export function toast(opts: Omit<Toast, "id">) {
  const id = genId();
  dispatch({ type: "ADD", toast: { id, ...opts } });
  setTimeout(() => dispatch({ type: "DISMISS", id }), 5000);
  return id;
}

export function useToast() {
  const [state, setState] = React.useState(memoryState);

  React.useEffect(() => {
    listeners.push(setState);
    return () => {
      const index = listeners.indexOf(setState);
      if (index > -1) listeners.splice(index, 1);
    };
  }, []);

  return {
    ...state,
    toast,
    dismiss: (id: string) => dispatch({ type: "DISMISS", id }),
  };
}
