"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[1033],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>m});var r=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,r,i=function(e,t){if(null==e)return{};var n,r,i={},l=Object.keys(e);for(r=0;r<l.length;r++)n=l[r],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)n=l[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=r.createContext({}),c=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},p=function(e){var t=c(e.components);return r.createElement(s.Provider,{value:t},e.children)},u="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,i=e.mdxType,l=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),u=c(n),d=i,m=u["".concat(s,".").concat(d)]||u[d]||g[d]||l;return n?r.createElement(m,a(a({ref:t},p),{},{components:n})):r.createElement(m,a({ref:t},p))}));function m(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var l=n.length,a=new Array(l);a[0]=d;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[u]="string"==typeof e?e:i,a[1]=o;for(var c=2;c<l;c++)a[c]=n[c];return r.createElement.apply(null,a)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},6168:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>a,default:()=>g,frontMatter:()=>l,metadata:()=>o,toc:()=>c});var r=n(7462),i=(n(7294),n(3905));const l={},a="Installing Plugin in Client",o={unversionedId:"client-guide/installing-plugin",id:"client-guide/installing-plugin",title:"Installing Plugin in Client",description:"Creating job specifications requires a plugin to be installed in the system caller. To simplify the installation,",source:"@site/docs/client-guide/installing-plugin.md",sourceDirName:"client-guide",slug:"/client-guide/installing-plugin",permalink:"/optimus/docs/client-guide/installing-plugin",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/installing-plugin.md",tags:[],version:"current",lastUpdatedBy:"Sandeep Bhardwaj",lastUpdatedAt:1682585441,formattedLastUpdatedAt:"Apr 27, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Managing Secrets",permalink:"/optimus/docs/client-guide/managing-secrets"},next:{title:"Manage BigQuery Resource",permalink:"/optimus/docs/client-guide/manage-bigquery-resource"}},s={},c=[],p={toc:c},u="wrapper";function g(e){let{components:t,...n}=e;return(0,i.kt)(u,(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"installing-plugin-in-client"},"Installing Plugin in Client"),(0,i.kt)("p",null,"Creating job specifications requires a plugin to be installed in the system caller. To simplify the installation,\nOptimus CLI can sync the YAML plugins supported and served by the Optimus server (with host as declared in project\nconfig) using the following command:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus plugin sync -c optimus.yaml\n")),(0,i.kt)("p",null,"Note: This will install plugins in the ",(0,i.kt)("inlineCode",{parentName:"p"},".plugins")," folder."))}g.isMDXComponent=!0}}]);