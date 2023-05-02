"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[1033],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>m});var i=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,i)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,i,r=function(e,t){if(null==e)return{};var n,i,r={},l=Object.keys(e);for(i=0;i<l.length;i++)n=l[i],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(i=0;i<l.length;i++)n=l[i],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=i.createContext({}),c=function(e){var t=i.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},u=function(e){var t=c(e.components);return i.createElement(s.Provider,{value:t},e.children)},p="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},d=i.forwardRef((function(e,t){var n=e.components,r=e.mdxType,l=e.originalType,s=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),p=c(n),d=r,m=p["".concat(s,".").concat(d)]||p[d]||g[d]||l;return n?i.createElement(m,a(a({ref:t},u),{},{components:n})):i.createElement(m,a({ref:t},u))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=n.length,a=new Array(l);a[0]=d;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[p]="string"==typeof e?e:r,a[1]=o;for(var c=2;c<l;c++)a[c]=n[c];return i.createElement.apply(null,a)}return i.createElement.apply(null,n)}d.displayName="MDXCreateElement"},6168:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>a,default:()=>g,frontMatter:()=>l,metadata:()=>o,toc:()=>c});var i=n(7462),r=(n(7294),n(3905));const l={},a="Installing Plugin in Client",o={unversionedId:"client-guide/installing-plugin",id:"client-guide/installing-plugin",title:"Installing Plugin in Client",description:"Creating job specifications requires a plugin to be installed in the system caller. To simplify the installation,",source:"@site/docs/client-guide/installing-plugin.md",sourceDirName:"client-guide",slug:"/client-guide/installing-plugin",permalink:"/optimus/docs/client-guide/installing-plugin",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/installing-plugin.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1683009677,formattedLastUpdatedAt:"May 2, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Managing Secrets",permalink:"/optimus/docs/client-guide/managing-secrets"},next:{title:"Manage BigQuery Resource",permalink:"/optimus/docs/client-guide/manage-bigquery-resource"}},s={},c=[],u={toc:c},p="wrapper";function g(e){let{components:t,...n}=e;return(0,r.kt)(p,(0,i.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"installing-plugin-in-client"},"Installing Plugin in Client"),(0,r.kt)("p",null,"Creating job specifications requires a plugin to be installed in the system caller. To simplify the installation,\nOptimus CLI can sync the YAML plugins supported and served by the Optimus server (with host as declared in project\nconfig) using the following command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus plugin sync -c optimus.yaml\n")),(0,r.kt)("p",null,"Note: This will install plugins in the ",(0,r.kt)("inlineCode",{parentName:"p"},".plugins")," folder."))}g.isMDXComponent=!0}}]);