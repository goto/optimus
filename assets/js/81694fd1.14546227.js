"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[6070],{3905:(e,n,t)=>{t.d(n,{Zo:()=>p,kt:()=>c});var i=t(7294);function a(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function r(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);n&&(i=i.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,i)}return t}function l(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?r(Object(t),!0).forEach((function(n){a(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):r(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function o(e,n){if(null==e)return{};var t,i,a=function(e,n){if(null==e)return{};var t,i,a={},r=Object.keys(e);for(i=0;i<r.length;i++)t=r[i],n.indexOf(t)>=0||(a[t]=e[t]);return a}(e,n);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(i=0;i<r.length;i++)t=r[i],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var s=i.createContext({}),u=function(e){var n=i.useContext(s),t=n;return e&&(t="function"==typeof e?e(n):l(l({},n),e)),t},p=function(e){var n=u(e.components);return i.createElement(s.Provider,{value:n},e.children)},m="mdxType",d={inlineCode:"code",wrapper:function(e){var n=e.children;return i.createElement(i.Fragment,{},n)}},g=i.forwardRef((function(e,n){var t=e.components,a=e.mdxType,r=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),m=u(t),g=a,c=m["".concat(s,".").concat(g)]||m[g]||d[g]||r;return t?i.createElement(c,l(l({ref:n},p),{},{components:t})):i.createElement(c,l({ref:n},p))}));function c(e,n){var t=arguments,a=n&&n.mdxType;if("string"==typeof e||a){var r=t.length,l=new Array(r);l[0]=g;var o={};for(var s in n)hasOwnProperty.call(n,s)&&(o[s]=n[s]);o.originalType=e,o[m]="string"==typeof e?e:a,l[1]=o;for(var u=2;u<r;u++)l[u]=t[u];return i.createElement.apply(null,l)}return i.createElement.apply(null,t)}g.displayName="MDXCreateElement"},6696:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>s,contentTitle:()=>l,default:()=>d,frontMatter:()=>r,metadata:()=>o,toc:()=>u});var i=t(7462),a=(t(7294),t(3905));const r={},l="Introduction of Plugin Development",o={unversionedId:"building-plugin/introduction",id:"building-plugin/introduction",title:"Introduction of Plugin Development",description:"As mentioned in the concepts, plugins provide support for various data warehouses & any",source:"@site/docs/building-plugin/introduction.md",sourceDirName:"building-plugin",slug:"/building-plugin/introduction",permalink:"/optimus/docs/building-plugin/introduction",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/building-plugin/introduction.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1725946924,formattedLastUpdatedAt:"Sep 10, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Defining Scheduler Version",permalink:"/optimus/docs/client-guide/defining-scheduler-version"},next:{title:"Tutorial of Plugin Development",permalink:"/optimus/docs/building-plugin/tutorial"}},s={},u=[{value:"Yaml Implementation of Plugin",id:"yaml-implementation-of-plugin",level:2},{value:"Limitations of Yaml plugins:",id:"limitations-of-yaml-plugins",level:3},{value:"Validating Yaml plugins:",id:"validating-yaml-plugins",level:3},{value:"Binary Implementation of Plugin (to be deprecated)",id:"binary-implementation-of-plugin-to-be-deprecated",level:2}],p={toc:u},m="wrapper";function d(e){let{components:n,...t}=e;return(0,a.kt)(m,(0,i.Z)({},p,t,{components:n,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"introduction-of-plugin-development"},"Introduction of Plugin Development"),(0,a.kt)("p",null,"As mentioned in the ",(0,a.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/plugin"},"concepts"),", plugins provide support for various data warehouses & any\nthird party system to handle all the data transformations or movement. Before we start, let\u2019s take a look at different\nimplementations of plugin: YAML and binary."),(0,a.kt)("h2",{id:"yaml-implementation-of-plugin"},"Yaml Implementation of Plugin"),(0,a.kt)("p",null,"Most plugins are expected to implement just the info and project side use-cases (mentioned above) and these are\ndata-driven i.e., plugin just provide data to Optimus. To simplify the development process of plugins, support for\nyaml mode of defining plugins is added."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-go"},'// representation of a yaml plugin schema in golang\n\n// below struct definition in golang can be marshalled\n// to generate yaml plugins\n\ntype YamlPlugin struct {\n// info use-case\nName          string `yaml:"name"`\nDescription   string `yaml:"description"`\nPlugintype    string `yaml:"plugintype"`\nPluginversion string `yaml:"pluginversion"`\nImage         string `yaml:"image"`\n\n    // survey use-case\n    Questions     []struct {\n        Name            string `yaml:"name"`\n        Prompt          string `yaml:"prompt"`\n        Help            string `yaml:"help"`\n        Regexp          string `yaml:"regexp"`\n        Validationerror string `yaml:"validationerror"`\n        Minlength       int    `yaml:"minlength"`\n        Required        bool     `yaml:"required,omitempty"`\n        Maxlength       int    `yaml:"maxlength,omitempty"`\n        Subquestions    []struct {\n            Ifvalue   string `yaml:"ifvalue"`\n            Questions []struct {\n                Name        string   `yaml:"name"`\n                Prompt      string   `yaml:"prompt"`\n                Help        string   `yaml:"help"`\n                Multiselect []string `yaml:"multiselect"`\n                Regexp          string `yaml:"regexp"`\n                Validationerror string `yaml:"validationerror"`\n                Minlength       int    `yaml:"minlength"`\n                Required        bool     `yaml:"required,omitempty"`\n                Maxlength       int    `yaml:"maxlength,omitempty"`\n            } `yaml:"questions"`\n        } `yaml:"subquestions,omitempty"`\n    } `yaml:"questions"`\n\n    // default-static-values use-case\n    Defaultassets []struct {\n        Name  string `yaml:"name"`\n        Value string `yaml:"value"`\n    } `yaml:"defaultassets"`\n    Defaultconfig []struct {\n        Name  string `yaml:"name"`\n        Value string `yaml:"value"`\n    } `yaml:"defaultconfig"`\n}\n')),(0,a.kt)("p",null,"Refer to sample implementation here."),(0,a.kt)("h3",{id:"limitations-of-yaml-plugins"},"Limitations of Yaml plugins:"),(0,a.kt)("p",null,"Here the scope of YAML plugins is limited to driving surveys, providing default values for job config and assets, and\nproviding plugin info. As the majority of the plugins are expected to implement a subset of these use cases, the\nsupport for YAML definitions for plugins is added which simplifies the development, packaging, and distribution of plugins."),(0,a.kt)("p",null,"For plugins that require enriching Optimus server-side behavior, YAML definitions fall short as this would require some code."),(0,a.kt)("h3",{id:"validating-yaml-plugins"},"Validating Yaml plugins:"),(0,a.kt)("p",null,"Also support for validating yaml plugin is added into optimus. After creating yaml definitions of plugin, one can\nvalidate them as below:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"optimus plugin validate --path {{directory of yaml plugins}}\n")),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"}," Note: The yaml plugin is expected to have file name as optimus-plugin-{{name}}.yaml\n")," Note: If Both yaml and binary plugin with same name are installed, Yaml implementation is prioritized over the\ncorresponding counterparts in binary implementation."),(0,a.kt)("h2",{id:"binary-implementation-of-plugin-to-be-deprecated"},"Binary Implementation of Plugin (to be deprecated)"),(0,a.kt)("p",null,"Binary implementations of Plugins are binaries which implement predefined protobuf interfaces to extend Optimus\nfunctionalities and augment the yaml implementations with executable code. Binary Plugins are implemented using\ngo-plugin developed by Hashicorp used in terraform and other similar products. Currently, Dependency Resolution Mod\nis the only interface that is supported in the binary approach to plugins."),(0,a.kt)("p",null,"Note : Binary plugins augment yaml plugins and they are not standalone."),(0,a.kt)("p",null,"Plugins can be implemented in any language as long as they can be exported as a single self-contained executable binary\nand implements a GRPC server. It is recommended to use Go currently for writing plugins because of its cross platform\nbuild functionality and to reuse protobuf sdk provided within Optimus core."),(0,a.kt)("p",null,(0,a.kt)("em",{parentName:"p"},"Binary Plugins can potentially modify the behavior of Optimus in undesired ways. Exercise caution when adding new\nplugins developed by unrecognized developers.")))}d.isMDXComponent=!0}}]);