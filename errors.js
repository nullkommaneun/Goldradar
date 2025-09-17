<!-- Diese Mini-Datei einfach im <head> oder direkt vor preflight/app einbinden -->
<script>
(function(){
  function w(s){try{
    var el=document.getElementById('diag'); 
    if(!el){return;}
    var t=new Date().toISOString().slice(11,19);
    el.textContent += "\n["+t+"] " + s;
  }catch(_){/* noop */}}
  window.addEventListener('error', function(e){
    var msg = "JS-Error: " + (e && e.message ? e.message : "unknown");
    if (e && e.filename) msg += " @ " + e.filename + (e.lineno? (":"+e.lineno):"");
    w(msg);
  });
  window.addEventListener('unhandledrejection', function(e){
    w("Unhandled promise: " + (e && e.reason ? (e.reason.message || String(e.reason)) : "unknown"));
  });
})();
</script>
