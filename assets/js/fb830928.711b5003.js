"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2685],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>b});var o=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,o,r=function(e,t){if(null==e)return{};var n,o,r={},a=Object.keys(e);for(o=0;o<a.length;o++)n=a[o],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(o=0;o<a.length;o++)n=a[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=o.createContext({}),p=function(e){var t=o.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},c=function(e){var t=p(e.components);return o.createElement(l.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},m=o.forwardRef((function(e,t){var n=e.components,r=e.mdxType,a=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),u=p(n),m=r,b=u["".concat(l,".").concat(m)]||u[m]||d[m]||a;return n?o.createElement(b,i(i({ref:t},c),{},{components:n})):o.createElement(b,i({ref:t},c))}));function b(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var a=n.length,i=new Array(a);i[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[u]="string"==typeof e?e:r,i[1]=s;for(var p=2;p<a;p++)i[p]=n[p];return o.createElement.apply(null,i)}return o.createElement.apply(null,n)}m.displayName="MDXCreateElement"},3460:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>d,frontMatter:()=>a,metadata:()=>s,toc:()=>p});var o=n(7462),r=(n(7294),n(3905));const a={},i="Webhooks",s={unversionedId:"concepts/webhook",id:"concepts/webhook",title:"Webhooks",description:"Overview:",source:"@site/docs/concepts/webhook.md",sourceDirName:"concepts",slug:"/concepts/webhook",permalink:"/optimus/docs/concepts/webhook",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/webhook.md",tags:[],version:"current",lastUpdatedBy:"Oky Setiawan",lastUpdatedAt:1722829864,formattedLastUpdatedAt:"Aug 5, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Replay & Backup",permalink:"/optimus/docs/concepts/replay-and-backup"},next:{title:"Server Configuration",permalink:"/optimus/docs/server-guide/configuration"}},l={},p=[{value:"Overview:",id:"overview",level:2},{value:"Scope:",id:"scope",level:2},{value:"On Events:",id:"on-events",level:3},{value:"WebHook-Request:",id:"webhook-request",level:2},{value:"Method:",id:"method",level:3}],c={toc:p},u="wrapper";function d(e){let{components:t,...n}=e;return(0,r.kt)(u,(0,o.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"webhooks"},"Webhooks"),(0,r.kt)("h2",{id:"overview"},"Overview:"),(0,r.kt)("p",null,"For Data Pipelines, there is often a requirement that systems external to Optimus expect notification/nudge upon job run state change. This helps people trigger external routines."),(0,r.kt)("h2",{id:"scope"},"Scope:"),(0,r.kt)("p",null,"Only HTTP/HTTPS webhooks are supported."),(0,r.kt)("h3",{id:"on-events"},"On Events:"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Event"),(0,r.kt)("th",{parentName:"tr",align:null},"Optimus Event Category"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Job Fail"),(0,r.kt)("td",{parentName:"tr",align:null},"'failure'")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Job Success"),(0,r.kt)("td",{parentName:"tr",align:null},"'success'")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Job SLA Miss"),(0,r.kt)("td",{parentName:"tr",align:null},"'sla_miss'")))),(0,r.kt)("h2",{id:"webhook-request"},"WebHook-Request:"),(0,r.kt)("h3",{id:"method"},"Method:"),(0,r.kt)("blockquote",null,(0,r.kt)("p",{parentName:"blockquote"},"HTTP POST"),(0,r.kt)("h3",{parentName:"blockquote",id:"url"},"URL:"),(0,r.kt)("p",{parentName:"blockquote"},"User configured"),(0,r.kt)("h3",{parentName:"blockquote",id:"request-content-type"},"Request Content-Type:"),(0,r.kt)("p",{parentName:"blockquote"},"application/json"),(0,r.kt)("h3",{parentName:"blockquote",id:"request-body"},"Request Body:"),(0,r.kt)("pre",{parentName:"blockquote"},(0,r.kt)("code",{parentName:"pre",className:"language-json"},'{\n   "job"            : "string",\n   "project"        : "string",\n   "namespace"      : "string",\n   "destination"    : "string",\n   "scheduled_at"   : "time-string:UTC",\n   "status"         : "string",\n   "job_label": {\n       "key"    : "value",\n       "key1"   : "value",\n       "key2"   : "value"\n   }\n}\n'))),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"\n### Expected Response\n- 200\n\n### On Error\n- Log it in Optimus Server\n\n\n## Add WebHooks in Job Specification\n```yaml\nversion: 1\nname: <JOB_NAME>\nowner: <OWNER>\nschedule:\n  start_date: \"2021-09-01\"\n  interval: 0 5 * * *\nbehavior:\n  webhook:\n    - on: success\n      endpoints:\n        - url: http://sub-domain.domain.com/path/to/the/webhook?some=somethingStatic\n          headers:\n            auth-header: '{{.secret.WEBHOOK_SECRET}}'\n            some_header: 'dummy value'\n    - on: sla_miss\n      endpoints:\n        - url: http://sub-domain.domain.com/path/to/the/webhook?some=somethingStatic\n    - on: failure\n      endpoints:\n        - url: http://sub-domain.domain.com/path/to/the/webhook1\n          headers:\n            auth: 'bearer: {{.secret.WEBHOOK_SECRET}}'\nnotify:\n  - on: failure\n    channels:\n      - slack://#somechannel\n      - pagerduty://#somechannel\ntask:\n  name: <task_name>\n  config:\n    labels:\n      label_name: <some_label>\ndependencies: []\n")))}d.isMDXComponent=!0}}]);