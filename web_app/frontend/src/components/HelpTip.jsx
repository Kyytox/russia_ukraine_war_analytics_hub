import { useId, useState } from "react";
import { CircleHelp } from "lucide-react";

export default function HelpTip({ children, label }) {
  const id = useId();
  const [open, setOpen] = useState(false); // pour le tactile et le clavier

  return (
    <span className="helptip">
      <button
        type="button"
        aria-label={label}
        aria-describedby={id}
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
        onBlur={() => setOpen(false)}
      >
        <CircleHelp size={18} />
      </button>
      <span
        id={id}
        role="tooltip"
        className={`helptip-box ${open ? "open" : ""}`}
      >
        {children}
      </span>
    </span>
  );
}
