import { useState } from "react";
import { useEngine } from "../engine/EngineContext";
import Collapsible from "./Collapsible";

export default function EngineTxForm() {
  const { sendTx } = useEngine();
  const [source, setSource]           = useState("world");
  const [destination, setDestination] = useState("alice");
  const [asset, setAsset]             = useState("USD/2");
  const [amount, setAmount]           = useState("100");

  function onSubmit(e: React.FormEvent): void {
    e.preventDefault();
    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) return;
    sendTx({
      ledger:      "demo",
      source,
      destination,
      asset,
      amount:      amt,
      reference:   `tx-${Date.now()}`,
    });
  }

  return (
    <Collapsible title="Send tx" wrapperClass="tx-form-panel">
      <form className="tx-form-inner" onSubmit={onSubmit}>
        <label className="tx-field">
          <span>source</span>
          <input value={source} onChange={e => setSource(e.target.value)} />
        </label>
        <label className="tx-field">
          <span>destination</span>
          <input value={destination} onChange={e => setDestination(e.target.value)} />
        </label>
        <label className="tx-field">
          <span>asset</span>
          <input value={asset} onChange={e => setAsset(e.target.value)} />
        </label>
        <label className="tx-field">
          <span>amount</span>
          <input type="number" min="1" value={amount} onChange={e => setAmount(e.target.value)} />
        </label>
        <div className="tx-actions">
          <button type="submit" className="ctrl-btn primary">▶ Send</button>
        </div>
      </form>
    </Collapsible>
  );
}
