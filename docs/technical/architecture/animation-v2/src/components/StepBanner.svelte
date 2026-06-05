<script>
  let { title = "", desc = "", color = "var(--grpc)" } = $props();
</script>

<div class="step-banner-html">
  <span class="step-dot-html" style="background: {color}; color: {color};"></span>
  <div class="step-content">
    <div class="step-title">{title}</div>
    <div class="step-desc">{desc}</div>
  </div>
</div>
