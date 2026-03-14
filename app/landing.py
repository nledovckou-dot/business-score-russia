"""Landing page HTML — light premium theme with РУССКОР radar logo."""

# SVG logo: radar pentagon (navy) with upward gold arrow — for light background
_LOGO_SVG = r'''<svg viewBox="0 0 200 210" width="72" height="76" xmlns="http://www.w3.org/2000/svg">
  <!-- Pentagon radar grid -->
  <g transform="translate(100,115)" fill="none" stroke="#1A2B4A" stroke-width="1.2">
    <polygon points="0,-55 52.3,-17 32.3,44.5 -32.3,44.5 -52.3,-17"/>
    <polygon points="0,-36.7 34.9,-11.3 21.5,29.7 -21.5,29.7 -34.9,-11.3"/>
    <polygon points="0,-18.3 17.4,-5.7 10.8,14.8 -10.8,14.8 -17.4,-5.7"/>
    <line x1="0" y1="0" x2="0" y2="-55"/>
    <line x1="0" y1="0" x2="52.3" y2="-17"/>
    <line x1="0" y1="0" x2="32.3" y2="44.5"/>
    <line x1="0" y1="0" x2="-32.3" y2="44.5"/>
    <line x1="0" y1="0" x2="-52.3" y2="-17"/>
  </g>
  <!-- Outer pentagon stroke -->
  <g transform="translate(100,115)" fill="none" stroke="#1A2B4A" stroke-width="2.5">
    <polygon points="0,-55 52.3,-17 32.3,44.5 -32.3,44.5 -52.3,-17"/>
  </g>
  <!-- Gold arrow -->
  <g transform="translate(100,115)" fill="#C9A44C" stroke="none">
    <polygon points="0,-95 15,-55 5,-55 5,-20 -5,-20 -5,-55 -15,-55"/>
  </g>
</svg>'''

LANDING_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>РУССКОР — Анализ бизнеса 360</title>
<style>
:root{
  --bg:#FAFBFD;--bg2:#F2F4F8;--bg3:#E8ECF2;
  --card:#FFFFFF;--card2:#F5F7FA;
  --border:#D8DEE8;--border2:#C0C8D8;
  --text:#1A2B4A;--text2:#4A5568;--text3:#8B99B3;
  --navy:#1A2B4A;--gold:#C9A44C;--gold2:#B8933F;
  --green:#2D9F5A;--red:#D44040;--orange:#D4880A
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,-apple-system,sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:0 20px}
.wrap{max-width:640px;width:100%}

/* ── Auth bar ── */
.auth-bar{position:fixed;top:0;right:0;left:0;display:flex;justify-content:flex-end;align-items:center;gap:8px;padding:8px 20px;z-index:100;background:rgba(250,251,253,0.92);backdrop-filter:blur(12px);border-bottom:1px solid var(--border)}
.auth-bar .auth-user{display:flex;align-items:center;gap:8px;font-size:0.82em;color:var(--text2)}
.auth-bar .auth-email{color:var(--navy);font-weight:500}
.auth-bar .quota-badge{padding:2px 8px;border-radius:10px;font-size:0.75em;font-weight:600;background:var(--bg3);color:var(--text2);border:1px solid var(--border)}
.auth-bar .quota-badge.depleted{background:rgba(212,64,64,0.1);color:var(--red);border-color:rgba(212,64,64,0.2)}
.btn-auth{padding:5px 12px;font-size:0.8em;border-radius:6px;cursor:pointer;font-family:inherit;border:none;transition:all 0.15s}
.btn-auth-login{background:transparent;color:var(--text2);border:1px solid var(--border) !important}
.btn-auth-login:hover{border-color:var(--navy) !important;color:var(--navy)}
.btn-auth-register{background:var(--navy);color:#fff;font-weight:600}
.btn-auth-register:hover{background:#243A5E}
.btn-auth-logout{background:transparent;color:var(--text3);font-size:0.75em !important}
.btn-auth-logout:hover{color:var(--red)}

/* ── Hero ── */
#phase-url{text-align:center;margin-top:18vh}
.logo{margin-bottom:20px;opacity:0;animation:logoIn 0.8s ease-out 0.2s forwards}
@keyframes logoIn{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:translateY(0)}}
h1{font-weight:300;font-size:1.9em;margin-bottom:8px;letter-spacing:-0.02em;color:var(--navy)}
h1 b{font-weight:600;color:var(--gold)}
.sub{color:var(--text2);font-size:0.9em;margin-bottom:32px;line-height:1.6}

/* ── Input ── */
.input-row{display:flex;gap:8px;margin-bottom:8px}
.input-row input{flex:1;padding:14px 16px;background:var(--card);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:0.95em;font-family:inherit;transition:border-color 0.2s;box-shadow:0 1px 3px rgba(0,0,0,0.04)}
.input-row input:focus{outline:none;border-color:var(--navy)}
.input-row input::placeholder{color:var(--text3)}
.btn{padding:14px 28px;background:var(--navy);border:none;border-radius:10px;color:#fff;font-size:0.95em;font-weight:600;cursor:pointer;font-family:inherit;white-space:nowrap;transition:all 0.2s;box-shadow:0 2px 8px rgba(26,43,74,0.15)}
.btn:hover{background:#243A5E}
.btn:disabled{opacity:0.3;cursor:not-allowed}
.hint{font-size:0.78em;color:var(--text3)}

/* ── Pipeline ── */
#phase-pipeline{display:none}
.pipeline-header{text-align:center;margin-bottom:20px}
.pipeline-header h2{font-size:1.1em;font-weight:500;margin-bottom:4px;color:var(--text)}
.pipeline-header .url-tag{color:var(--text3);font-size:0.82em}
.steps{border:1px solid var(--border);border-radius:12px;margin-bottom:16px;overflow:hidden;background:var(--card);box-shadow:0 1px 4px rgba(0,0,0,0.04)}
.step{display:flex;align-items:center;gap:10px;padding:11px 18px;font-size:0.85em;color:var(--text3);border-bottom:1px solid var(--border);transition:color 0.2s}
.step:last-child{border-bottom:none}
.step.active{color:var(--navy);font-weight:500}
.step.done{color:var(--green)}
.step.fail{color:var(--red)}
.step.warning{color:var(--orange)}
.step-icon{width:22px;height:22px;border-radius:50%;border:1.5px solid currentColor;display:flex;align-items:center;justify-content:center;font-size:0.7em;flex-shrink:0}
.step.active .step-icon{animation:pulse 1.5s infinite}
.step.done .step-icon{background:var(--green);border-color:var(--green);color:#fff}
.step.fail .step-icon{background:var(--red);border-color:var(--red);color:#fff}
.step.warning .step-icon{background:var(--orange);border-color:var(--orange);color:#fff}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}
.step-group{padding:6px 18px 4px;font-size:0.72em;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:0.06em;background:var(--bg2);border-bottom:1px solid var(--border)}

/* ── Panels ── */
.panel{border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:16px;display:none;background:var(--card);box-shadow:0 1px 4px rgba(0,0,0,0.04)}
.panel h3{font-size:0.95em;font-weight:600;margin-bottom:14px;color:var(--navy)}
.field{margin-bottom:12px}
.field label{display:block;font-size:0.75em;color:var(--text3);margin-bottom:4px;text-transform:uppercase;letter-spacing:0.03em}
.field input,.field select{width:100%;padding:10px 12px;border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:0.9em;font-family:inherit;background:var(--card)}
.field input:focus,.field select:focus{outline:none;border-color:var(--navy)}
.field-row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.fns-info{background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:12px 14px;margin-bottom:14px;font-size:0.85em;color:#166534}
.fns-warning{background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 14px;margin-bottom:14px;font-size:0.85em;color:#92400e}

/* ── Competitors ── */
.comp-list{display:flex;flex-direction:column;gap:8px;margin-bottom:14px}
.comp-item{display:flex;align-items:center;gap:12px;border:1px solid var(--border);border-radius:8px;padding:12px 14px;background:var(--card);transition:border-color 0.15s}
.comp-item:hover{border-color:var(--border2)}
.comp-item.excluded{opacity:0.35;border-style:dashed}
.comp-toggle{width:20px;height:20px;border-radius:5px;border:1.5px solid var(--border);background:none;cursor:pointer;display:flex;align-items:center;justify-content:center;color:var(--green);font-size:0.85em;flex-shrink:0}
.comp-toggle.on{background:var(--green);border-color:var(--green);color:#fff}
.comp-info{flex:1;min-width:0}
.comp-name{font-weight:600;font-size:0.9em;margin-bottom:1px;color:var(--navy)}
.comp-desc{font-size:0.78em;color:var(--text3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.comp-threat{font-size:0.72em;font-weight:600;padding:2px 8px;border-radius:10px;flex-shrink:0}
.threat-high{background:#fef2f2;color:var(--red)}
.threat-med{background:#fffbeb;color:var(--orange)}
.threat-low{background:#f0fdf4;color:var(--green)}

/* ── Result ── */
.result{display:none;text-align:center;padding:32px 24px;border:1px solid var(--border);border-radius:12px;background:var(--card);box-shadow:0 2px 12px rgba(26,43,74,0.08)}
.result h3{font-weight:500;font-size:1.15em;margin-bottom:4px;color:var(--navy)}
.result .company{color:var(--text2);font-size:0.85em;margin-bottom:16px}
.result a{display:inline-block;padding:12px 36px;background:var(--navy);color:#fff;font-weight:600;font-size:0.95em;border-radius:10px;text-decoration:none;transition:all 0.2s;box-shadow:0 2px 8px rgba(26,43,74,0.15)}
.result a:hover{background:#243A5E}
.result .meta{color:var(--text3);font-size:0.75em;margin-top:12px}

/* ── Error ── */
.error{display:none;padding:12px 16px;background:#fef2f2;border:1px solid #fecaca;border-radius:8px;color:var(--red);font-size:0.85em;margin-bottom:14px}
.again{display:inline-block;margin-top:14px;color:var(--text3);font-size:0.82em;cursor:pointer;text-decoration:underline;border:none;background:none;font-family:inherit}
.again:hover{color:var(--navy)}

/* ── Modals ── */
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.25);backdrop-filter:blur(4px);z-index:200;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal{background:#fff;border:1px solid var(--border);border-radius:14px;padding:28px 24px;width:100%;max-width:380px;position:relative;box-shadow:0 8px 32px rgba(0,0,0,0.1)}
.modal h2{font-size:1.1em;font-weight:600;margin-bottom:4px;color:var(--navy)}
.modal .modal-sub{font-size:0.82em;color:var(--text2);margin-bottom:20px}
.modal .modal-close{position:absolute;top:14px;right:14px;background:none;border:none;color:var(--text3);font-size:1.1em;cursor:pointer;padding:4px 8px;border-radius:6px}
.modal .modal-close:hover{color:var(--navy);background:var(--bg3)}
.modal .field{margin-bottom:14px}
.modal .modal-error{display:none;padding:8px 12px;background:#fef2f2;border:1px solid #fecaca;border-radius:6px;color:var(--red);font-size:0.82em;margin-bottom:14px}
.modal .modal-error.visible{display:block}
.modal .btn-full{width:100%;padding:11px;background:var(--navy);border:none;border-radius:8px;color:#fff;font-size:0.9em;font-weight:600;cursor:pointer;font-family:inherit}
.modal .btn-full:hover{background:#243A5E}
.modal .btn-full:disabled{opacity:0.3;cursor:not-allowed}
.modal .modal-switch{text-align:center;margin-top:14px;font-size:0.8em;color:var(--text3)}
.modal .modal-switch a{color:var(--navy);cursor:pointer;text-decoration:none}
.modal .modal-switch a:hover{text-decoration:underline}

/* ── Footer ── */
.footer{text-align:center;padding:32px 0 16px;color:var(--text3);font-size:0.75em;margin-top:auto;width:100%}

/* ── Background subtle ── */

@media(max-width:600px){
    #phase-url{margin-top:10vh}
    h1{font-size:1.4em}
    .input-row{flex-direction:column}
    .input-row .btn{width:100%}
    .field-row{grid-template-columns:1fr}
    .auth-bar{padding:6px 12px}
    .modal{margin:16px;padding:24px 18px}
}
</style>
</head>
<body>

<div class="auth-bar" id="auth-bar">
    <div id="auth-guest">
        <button class="btn-auth btn-auth-login" onclick="openModal('login')">Войти</button>
        <button class="btn-auth btn-auth-register" onclick="openModal('register')">Регистрация</button>
    </div>
    <div id="auth-logged" style="display:none" class="auth-user">
        <span class="auth-email" id="auth-email"></span>
        <span class="quota-badge" id="auth-quota"></span>
        <button class="btn-auth btn-auth-logout" onclick="doLogout()">Выйти</button>
    </div>
</div>

<div class="modal-overlay" id="modal-login">
    <div class="modal">
        <button class="modal-close" onclick="closeModal('login')">&times;</button>
        <h2>Войти</h2>
        <p class="modal-sub">Войдите, чтобы сохранять отчёты</p>
        <div class="modal-error" id="login-error"></div>
        <div class="field"><label>Email</label><input id="login-email" type="email" placeholder="name@example.com" onkeydown="if(event.key==='Enter')doLogin()"></div>
        <div class="field"><label>Пароль</label><input id="login-password" type="password" placeholder="Минимум 6 символов" onkeydown="if(event.key==='Enter')doLogin()"></div>
        <button class="btn-full" id="login-btn" onclick="doLogin()">Войти</button>
        <div class="modal-switch">Нет аккаунта? <a onclick="closeModal('login');openModal('register')">Регистрация</a></div>
    </div>
</div>

<div class="modal-overlay" id="modal-register">
    <div class="modal">
        <button class="modal-close" onclick="closeModal('register')">&times;</button>
        <h2>Регистрация</h2>
        <p class="modal-sub">5 бесплатных отчётов</p>
        <div class="modal-error" id="register-error"></div>
        <div class="field"><label>Email</label><input id="register-email" type="email" placeholder="name@example.com" onkeydown="if(event.key==='Enter')doRegister()"></div>
        <div class="field"><label>Пароль</label><input id="register-password" type="password" placeholder="Минимум 6 символов" onkeydown="if(event.key==='Enter')doRegister()"></div>
        <button class="btn-full" id="register-btn" onclick="doRegister()">Создать аккаунт</button>
        <div class="modal-switch">Есть аккаунт? <a onclick="closeModal('register');openModal('login')">Войти</a></div>
    </div>
</div>

<div class="wrap" style="padding-top:52px;position:relative;z-index:1">
    <div id="phase-url">
        <div class="logo">""" + _LOGO_SVG + r"""</div>
        <h1>Анализ <b>бизнеса</b> 360&deg;</h1>
        <p class="sub">Вставьте ссылку на сайт компании — получите полный отчёт<br>с финансами, конкурентами и стратегией</p>
        <div class="input-row">
            <input id="url" type="url" placeholder="https://example.com" autofocus onkeydown="if(event.key==='Enter')startAnalysis()">
            <button class="btn" id="gobtn" onclick="startAnalysis()">Анализировать</button>
        </div>
        <div class="hint">ФНС, ЕГРЮЛ, HH.ru, открытые источники</div>
    </div>

    <div id="phase-pipeline">
        <div class="pipeline-header">
            <h2>Анализ</h2>
            <div class="url-tag" id="url-tag"></div>
        </div>

        <div class="steps">
            <div class="step-group">Сбор данных</div>
            <div class="step" id="s0"><div class="step-icon">&middot;</div><span>Выбор AI-моделей</span></div>
            <div class="step" id="s1"><div class="step-icon">1</div><span>Загрузка сайта</span></div>
            <div class="step" id="s2"><div class="step-icon">2</div><span>Определение компании</span></div>
            <div class="step" id="s3"><div class="step-icon">3</div><span>Поиск в ФНС</span></div>
            <div class="step-group">Конкуренты</div>
            <div class="step" id="s4"><div class="step-icon">4</div><span>Поиск конкурентов</span></div>
            <div class="step-group">Анализ</div>
            <div class="step" id="s1b"><div class="step-icon">&middot;</div><span>Маркетплейсы</span></div>
            <div class="step" id="s1c"><div class="step-icon">&middot;</div><span>Бизнес-модели и каналы</span></div>
            <div class="step" id="s4h"><div class="step-icon">&middot;</div><span>HR и вакансии</span></div>
            <div class="step" id="s5"><div class="step-icon">5</div><span>Глубокий анализ</span></div>
            <div class="step-group">Проверка</div>
            <div class="step" id="s2a"><div class="step-icon">&middot;</div><span>Верификация</span></div>
            <div class="step" id="s2b"><div class="step-icon">&middot;</div><span>Relevance Gate</span></div>
            <div class="step" id="s6a"><div class="step-icon">&middot;</div><span>Совет директоров</span></div>
            <div class="step" id="sqa"><div class="step-icon">&middot;</div><span>Проверка качества</span></div>
            <div class="step" id="s7"><div class="step-icon">&middot;</div><span>Сборка отчёта</span></div>
        </div>

        <div class="panel" id="panel-company">
            <h3>Подтвердите компанию</h3>
            <div id="fns-status"></div>
            <div class="field-row">
                <div class="field"><label>Название</label><input id="c-name" type="text"></div>
                <div class="field"><label>ИНН</label><input id="c-inn" type="text" placeholder="10 или 12 цифр"></div>
            </div>
            <div class="field-row">
                <div class="field"><label>Юрлицо</label><input id="c-legal" type="text" placeholder='ООО "..."'></div>
                <div class="field">
                    <label>Тип бизнеса</label>
                    <select id="c-type">
                        <option value="B2C_SERVICE">B2C Услуги</option>
                        <option value="B2C_PRODUCT">B2C Товары</option>
                        <option value="B2B_SERVICE">B2B Услуги</option>
                        <option value="B2B_PRODUCT">B2B Товары</option>
                        <option value="PLATFORM">Платформа</option>
                        <option value="B2B_B2C_HYBRID">B2B+B2C Гибрид</option>
                    </select>
                </div>
            </div>
            <div class="field"><label>Адрес</label><input id="c-address" type="text"></div>
            <div style="margin-top:8px"><button class="btn" onclick="confirmCompany()">Подтвердить</button></div>
        </div>

        <div class="panel" id="panel-competitors">
            <h3>Конкуренты <span id="market-name" style="font-weight:400;color:var(--text3);font-size:0.85em"></span></h3>
            <p style="font-size:0.82em;color:var(--text2);margin-bottom:14px">Уберите нерелевантных</p>
            <div class="comp-list" id="comp-list"></div>
            <button class="btn" onclick="confirmCompetitors()">Подтвердить и запустить</button>
        </div>

        <div class="error" id="error"></div>

        <div class="result" id="result">
            <h3>Отчёт готов</h3>
            <div class="company" id="rcompany"></div>
            <a id="rlink" href="#" target="_blank">Открыть отчёт</a>
            <div class="meta" id="rmeta"></div>
            <button class="again" onclick="location.reload()">Новый анализ</button>
        </div>
    </div>
</div>

<footer class="footer">РУССКОР <span id="app-ver"></span></footer>
<script>fetch('/api/health').then(r=>r.json()).then(d=>{document.getElementById('app-ver').textContent='v'+(d.version||'?')})</script>

<script>
var authUser=null;
function openModal(t){document.getElementById('modal-'+t).classList.add('open');var i=document.querySelector('#modal-'+t+' input');if(i)setTimeout(function(){i.focus()},100)}
function closeModal(t){document.getElementById('modal-'+t).classList.remove('open');var e=document.getElementById(t+'-error');if(e){e.classList.remove('visible');e.textContent=''}}
function showModalError(t,m){var e=document.getElementById(t+'-error');e.textContent=m;e.classList.add('visible')}
function updateAuthUI(){
    if(authUser){
        document.getElementById('auth-guest').style.display='none';
        document.getElementById('auth-logged').style.display='flex';
        document.getElementById('auth-email').textContent=authUser.email;
        var q=document.getElementById('auth-quota');
        q.textContent=authUser.reports_remaining+'/5';
        q.className='quota-badge'+(authUser.reports_remaining<=0?' depleted':'');
    }else{
        document.getElementById('auth-guest').style.display='flex';
        document.getElementById('auth-logged').style.display='none';
    }
}
function doRegister(){
    var e=document.getElementById('register-email').value.trim(),p=document.getElementById('register-password').value;
    if(!e||!p){showModalError('register','Заполните все поля');return}
    var b=document.getElementById('register-btn');b.disabled=true;
    fetch('/api/auth/register',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:e,password:p})})
    .then(function(r){return r.json()}).then(function(r){b.disabled=false;if(!r.ok){showModalError('register',r.error||'Ошибка');return}
        authUser={email:r.email,reports_used:r.reports_used,reports_remaining:r.reports_remaining};updateAuthUI();closeModal('register')})
    .catch(function(err){b.disabled=false;showModalError('register','Ошибка: '+err.message)})
}
function doLogin(){
    var e=document.getElementById('login-email').value.trim(),p=document.getElementById('login-password').value;
    if(!e||!p){showModalError('login','Заполните все поля');return}
    var b=document.getElementById('login-btn');b.disabled=true;
    fetch('/api/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:e,password:p})})
    .then(function(r){return r.json()}).then(function(r){b.disabled=false;if(!r.ok){showModalError('login',r.error||'Ошибка');return}
        authUser={email:r.email,reports_used:r.reports_used,reports_remaining:r.reports_remaining};updateAuthUI();closeModal('login')})
    .catch(function(err){b.disabled=false;showModalError('login','Ошибка: '+err.message)})
}
function doLogout(){fetch('/api/auth/logout',{method:'POST'}).then(function(){authUser=null;updateAuthUI()}).catch(function(){authUser=null;updateAuthUI()})}
function checkAuth(){fetch('/api/auth/me').then(function(r){return r.json()}).then(function(r){if(r.ok&&r.authenticated)authUser={email:r.email,reports_used:r.reports_used,reports_remaining:r.reports_remaining};updateAuthUI()}).catch(function(){updateAuthUI()})}
document.addEventListener('click',function(e){if(e.target.classList.contains('modal-overlay'))e.target.classList.remove('open')});
document.addEventListener('keydown',function(e){if(e.key==='Escape')document.querySelectorAll('.modal-overlay.open').forEach(function(m){m.classList.remove('open')})});
checkAuth();

var SID=null,evtSource=null,competitorData=[];

function startAnalysis(){
    var url=document.getElementById('url').value.trim();
    if(!url){document.getElementById('url').focus();return}
    if(!url.match(/^https?:\/\//))url='https://'+url;
    document.getElementById('gobtn').disabled=true;
    document.getElementById('phase-url').style.display='none';
    document.getElementById('phase-pipeline').style.display='block';
    document.getElementById('url-tag').textContent=url;
    fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:url})})
    .then(function(r){return r.json()}).then(function(r){
        if(!r.ok){showError(r.error);return}
        SID=r.session_id;listenSSE();
    }).catch(function(err){showError('Ошибка сети: '+err.message)})
}

function listenSSE(){
    evtSource=new EventSource('/api/events/'+SID);
    evtSource.addEventListener('step',function(e){var d=JSON.parse(e.data);setStep(d.num,d.status,d.text)});
    evtSource.addEventListener('waiting_company',function(e){showCompanyPanel(JSON.parse(e.data))});
    evtSource.addEventListener('waiting_competitors',function(e){showCompetitorPanel(JSON.parse(e.data))});
    evtSource.addEventListener('done',function(e){
        var d=JSON.parse(e.data);evtSource.close();
        document.getElementById('result').style.display='block';
        document.getElementById('rcompany').textContent=d.company||'';
        document.getElementById('rlink').href=d.url;
        document.getElementById('rmeta').textContent=d.size_kb+' KB';
        if(authUser&&d.reports_remaining!==undefined){authUser.reports_remaining=d.reports_remaining;authUser.reports_used=5-d.reports_remaining;updateAuthUI()}
    });
    evtSource.addEventListener('error',function(e){
        try{var d=JSON.parse(e.data);showError(d.message||'Ошибка')}catch(ex){showError('Соединение потеряно')}
        evtSource.close();
    });
}

function showCompanyPanel(d){
    var ci=d.company_info||{},fns=d.fns_data||{},fc=fns.fns_company||{};
    document.getElementById('c-name').value=ci.name||fc.name||'';
    document.getElementById('c-inn').value=fc.inn||ci.inn||'';
    document.getElementById('c-legal').value=fc.full_name||ci.legal_name||'';
    document.getElementById('c-address').value=fc.address||ci.address||'';
    var bt=ci.business_type_guess||'B2B_SERVICE',sel=document.getElementById('c-type');
    for(var i=0;i<sel.options.length;i++){if(sel.options[i].value===bt)sel.selectedIndex=i}
    var s=document.getElementById('fns-status');
    if(fc.inn){s.className='fns-info';s.innerHTML='\u2713 ФНС: '+(fc.name||'')+' | ИНН '+fc.inn+(fc.okved?' | ОКВЭД '+fc.okved:'')}
    else{s.className='fns-warning';s.textContent='\u26A0 Юрлицо не найдено. Введите ИНН вручную.'}
    var mm=fns.entity_mismatch;
    if(mm&&mm.has_mismatch){s.innerHTML+='<br><span style="color:var(--orange)">\u26A0 Несовпадение: сайт \u00AB'+mm.brand_name+'\u00BB \u2192 юрлицо \u00AB'+mm.legal_name+'\u00BB. Финансы ФНС могут включать другие продукты.</span>'}
    document.getElementById('panel-company').style.display='block';
}

function confirmCompany(){
    document.getElementById('panel-company').style.display='none';
    fetch('/api/confirm-company/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({name:document.getElementById('c-name').value,inn:document.getElementById('c-inn').value,
            legal_name:document.getElementById('c-legal').value,address:document.getElementById('c-address').value,
            business_type_guess:document.getElementById('c-type').value})});
}

function showCompetitorPanel(d){
    competitorData=d.competitors||[];
    document.getElementById('market-name').textContent=d.market_name?'| '+d.market_name:'';
    renderCompetitors();
    document.getElementById('panel-competitors').style.display='block';
}

function renderCompetitors(){
    var h='';
    for(var i=0;i<competitorData.length;i++){
        var c=competitorData[i],on=c._enabled!==false;
        var tCls='threat-'+(c.threat_level||'med');
        var vb='';
        if(c.verified===false)vb='<span style="font-size:0.7em;color:var(--orange);margin-left:4px">\u26A0</span>';
        else if(c.verification_confidence==='high')vb='<span style="font-size:0.7em;color:var(--green);margin-left:4px">\uD83D\uDD12</span>';
        else if(c.verification_confidence==='medium')vb='<span style="font-size:0.7em;color:var(--green);margin-left:4px">\u2713</span>';
        h+='<div class="comp-item'+(on?'':' excluded')+'">'+'<button class="comp-toggle '+(on?'on':'')+'" onclick="toggleComp('+i+')">'+(on?'\u2713':'')+'</button>'+'<div class="comp-info"><div class="comp-name">'+(c.name||'')+vb+'</div>'+'<div class="comp-desc">'+(c.description||c.why_competitor||'')+'</div></div>'+'<span class="comp-threat '+tCls+'">'+(c.threat_level||'med')+'</span></div>';
    }
    document.getElementById('comp-list').innerHTML=h;
}

function toggleComp(i){competitorData[i]._enabled=competitorData[i]._enabled===false?true:false;renderCompetitors()}

function confirmCompetitors(){
    var sel=competitorData.filter(function(c){return c._enabled!==false});
    document.getElementById('panel-competitors').style.display='none';
    fetch('/api/confirm-competitors/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({competitors:sel})});
}

function setStep(n,status,text){
    var el=document.getElementById('s'+n);
    if(!el)return;
    el.className='step '+status;
    if(text)el.querySelector('span').textContent=text;
    var icon=el.querySelector('.step-icon');
    if(status==='done')icon.textContent='\u2713';
    else if(status==='fail')icon.textContent='\u2717';
    else if(status==='warning')icon.textContent='!';
}

function showError(m){var e=document.getElementById('error');e.style.display='block';e.textContent=m}
</script>
</body>
</html>"""
