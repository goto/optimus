"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[6173],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>g});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var p=r.createContext({}),s=function(e){var t=r.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},u=function(e){var t=s(e.components);return r.createElement(p.Provider,{value:t},e.children)},c="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,p=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),c=s(n),m=a,g=c["".concat(p,".").concat(m)]||c[m]||d[m]||i;return n?r.createElement(g,o(o({ref:t},u),{},{components:n})):r.createElement(g,o({ref:t},u))}));function g(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,o=new Array(i);o[0]=m;var l={};for(var p in t)hasOwnProperty.call(t,p)&&(l[p]=t[p]);l.originalType=e,l[c]="string"==typeof e?e:a,o[1]=l;for(var s=2;s<i;s++)o[s]=n[s];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},7217:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>l,toc:()=>s});var r=n(7462),a=(n(7294),n(3905));const i={},o="Setting up Alert to Job",l={unversionedId:"client-guide/setting-up-alert",id:"client-guide/setting-up-alert",title:"Setting up Alert to Job",description:"There are chances that your job is failing due to some reason or missed the SLA. For these cases, you might want to set",source:"@site/docs/client-guide/setting-up-alert.md",sourceDirName:"client-guide",slug:"/client-guide/setting-up-alert",permalink:"/optimus/docs/client-guide/setting-up-alert",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/setting-up-alert.md",tags:[],version:"current",lastUpdatedBy:"Sandeep Bhardwaj",lastUpdatedAt:1691570012,formattedLastUpdatedAt:"Aug 9, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Create Job Specifications",permalink:"/optimus/docs/client-guide/create-job-specifications"},next:{title:"Verifying the Jobs",permalink:"/optimus/docs/client-guide/verifying-jobs"}},p={},s=[{value:"Supported Events",id:"supported-events",level:2},{value:"Supported Channels",id:"supported-channels",level:2},{value:"Sample Configuration",id:"sample-configuration",level:2}],u={toc:s},c="wrapper";function d(e){let{components:t,...n}=e;return(0,a.kt)(c,(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"setting-up-alert-to-job"},"Setting up Alert to Job"),(0,a.kt)("p",null,"There are chances that your job is failing due to some reason or missed the SLA. For these cases, you might want to set\nthe alerts and get notified as soon as possible."),(0,a.kt)("h2",{id:"supported-events"},"Supported Events"),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Event Type"),(0,a.kt)("th",{parentName:"tr",align:null},"Description"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"failure"),(0,a.kt)("td",{parentName:"tr",align:null},"Triggered when job run status is failed.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"sla_miss"),(0,a.kt)("td",{parentName:"tr",align:null},"Triggered when the job run does not complete within the duration that you expected. Duration should be specified in the config and should be in string duration.")))),(0,a.kt)("h2",{id:"supported-channels"},"Supported Channels"),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Channel"),(0,a.kt)("th",{parentName:"tr",align:null},"Description"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Slack"),(0,a.kt)("td",{parentName:"tr",align:null},"Channel/team handle or specific user")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Pagerduty"),(0,a.kt)("td",{parentName:"tr",align:null},"Needing ",(0,a.kt)("inlineCode",{parentName:"td"},"notify_<pagerduty_service_name>")," secret with pagerduty integration key/routing key")))),(0,a.kt)("h2",{id:"sample-configuration"},"Sample Configuration"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"behavior:\nnotify:\n- 'on': failure/sla_miss\n  config :\n    duration : 2h45m\n  channels:\n    - slack://#slack-channel or @team-group or user&gmail.com\n    - pagerduty://#pagerduty_service_name\n")))}d.isMDXComponent=!0}}]);